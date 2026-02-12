"""
Enhanced VOD Connection Manager with Redis-based connection sharing for multi-worker environments
"""

import time
import json
import logging
import threading
import random
import re
import requests
import pickle
import base64
import os
import socket
import mimetypes
from urllib.parse import urlparse
from typing import Optional, Dict, Any
from django.http import StreamingHttpResponse, HttpResponse
from core.utils import RedisClient
from apps.vod.models import Movie, Episode
from apps.m3u.models import M3UAccountProfile

logger = logging.getLogger("vod_proxy")


def get_vod_client_stop_key(client_id):
    """Get the Redis key for signaling a VOD client to stop"""
    return f"vod_proxy:client:{client_id}:stop"


def infer_content_type_from_url(url: str) -> Optional[str]:
    """
    Infer MIME type from file extension in URL

    Args:
        url: The stream URL

    Returns:
        MIME type string or None if cannot be determined
    """
    try:
        parsed_url = urlparse(url)
        path = parsed_url.path

        # Extract file extension
        _, ext = os.path.splitext(path)
        ext = ext.lower()

        # Common video format mappings
        video_mime_types = {
            '.mp4': 'video/mp4',
            '.mkv': 'video/x-matroska',
            '.avi': 'video/x-msvideo',
            '.mov': 'video/quicktime',
            '.wmv': 'video/x-ms-wmv',
            '.flv': 'video/x-flv',
            '.webm': 'video/webm',
            '.m4v': 'video/x-m4v',
            '.3gp': 'video/3gpp',
            '.ts': 'video/mp2t',
            '.m3u8': 'application/x-mpegURL',
            '.mpg': 'video/mpeg',
            '.mpeg': 'video/mpeg',
        }

        if ext in video_mime_types:
            logger.debug(f"Inferred content type '{video_mime_types[ext]}' from extension '{ext}' in URL: {url}")
            return video_mime_types[ext]

        # Fallback to mimetypes module
        mime_type, _ = mimetypes.guess_type(path)
        if mime_type and mime_type.startswith('video/'):
            logger.debug(f"Inferred content type '{mime_type}' using mimetypes for URL: {url}")
            return mime_type

        logger.debug(f"Could not infer content type from URL: {url}")
        return None

    except Exception as e:
        logger.warning(f"Error inferring content type from URL '{url}': {e}")
        return None


class SerializableConnectionState:
    """Serializable connection state that can be stored in Redis"""

    def __init__(self, session_id: str, stream_url: str, headers: dict,
                 content_length: str = None, content_type: str = None,
                 final_url: str = None, m3u_profile_id: int = None,
                 # Session metadata fields (previously stored in vod_session key)
                 content_obj_type: str = None, content_uuid: str = None,
                 content_name: str = None, client_ip: str = None,
                 client_user_agent: str = None, utc_start: str = None,
                 utc_end: str = None, offset: str = None,
                 worker_id: str = None, connection_type: str = "redis_backed"):
        self.session_id = session_id
        self.stream_url = stream_url
        self.headers = headers
        self.content_length = content_length
        self.content_type = content_type
        self.final_url = final_url
        self.m3u_profile_id = m3u_profile_id  # Store M3U profile ID for connection counting
        self.last_activity = time.time()
        self.request_count = 0
        self.active_streams = 0

        # Session metadata (consolidated from vod_session key)
        self.content_obj_type = content_obj_type
        self.content_uuid = content_uuid
        self.content_name = content_name
        self.client_ip = client_ip
        self.client_user_agent = client_user_agent
        self.utc_start = utc_start or ""
        self.utc_end = utc_end or ""
        self.offset = offset or ""
        self.worker_id = worker_id
        self.connection_type = connection_type
        self.created_at = time.time()

        # Additional tracking fields
        self.bytes_sent = 0
        self.position_seconds = 0

        # Range/seek tracking for position calculation
        self.last_seek_byte = 0
        self.last_seek_percentage = 0.0
        self.total_content_size = 0
        self.last_seek_timestamp = 0.0

    def to_dict(self):
        """Convert to dictionary for Redis storage"""
        return {
            'session_id': self.session_id or '',
            'stream_url': self.stream_url or '',
            'headers': json.dumps(self.headers or {}),
            'content_length': str(self.content_length) if self.content_length is not None else '',
            'content_type': self.content_type or '',
            'final_url': self.final_url or '',
            'm3u_profile_id': str(self.m3u_profile_id) if self.m3u_profile_id is not None else '',
            'last_activity': str(self.last_activity),
            'request_count': str(self.request_count),
            'active_streams': str(self.active_streams),
            # Session metadata
            'content_obj_type': self.content_obj_type or '',
            'content_uuid': self.content_uuid or '',
            'content_name': self.content_name or '',
            'client_ip': self.client_ip or '',
            'client_user_agent': self.client_user_agent or '',
            'utc_start': self.utc_start or '',
            'utc_end': self.utc_end or '',
            'offset': self.offset or '',
            'worker_id': self.worker_id or '',
            'connection_type': self.connection_type or 'redis_backed',
            'created_at': str(self.created_at),
            # Additional tracking fields
            'bytes_sent': str(self.bytes_sent),
            'position_seconds': str(self.position_seconds),
            # Range/seek tracking
            'last_seek_byte': str(self.last_seek_byte),
            'last_seek_percentage': str(self.last_seek_percentage),
            'total_content_size': str(self.total_content_size),
            'last_seek_timestamp': str(self.last_seek_timestamp)
        }

    @classmethod
    def from_dict(cls, data: dict):
        """Create from dictionary loaded from Redis"""
        obj = cls(
            session_id=data['session_id'],
            stream_url=data['stream_url'],
            headers=json.loads(data['headers']) if data['headers'] else {},
            content_length=data.get('content_length') if data.get('content_length') else None,
            content_type=data.get('content_type') or None,
            final_url=data.get('final_url') if data.get('final_url') else None,
            m3u_profile_id=int(data.get('m3u_profile_id')) if data.get('m3u_profile_id') else None,
            # Session metadata
            content_obj_type=data.get('content_obj_type') or None,
            content_uuid=data.get('content_uuid') or None,
            content_name=data.get('content_name') or None,
            client_ip=data.get('client_ip') or None,
            client_user_agent=data.get('client_user_agent') or data.get('user_agent') or None,
            utc_start=data.get('utc_start') or '',
            utc_end=data.get('utc_end') or '',
            offset=data.get('offset') or '',
            worker_id=data.get('worker_id') or None,
            connection_type=data.get('connection_type', 'redis_backed')
        )
        obj.last_activity = float(data.get('last_activity', time.time()))
        obj.request_count = int(data.get('request_count', 0))
        obj.active_streams = int(data.get('active_streams', 0))
        obj.created_at = float(data.get('created_at', time.time()))
        # Additional tracking fields
        obj.bytes_sent = int(data.get('bytes_sent', 0))
        obj.position_seconds = int(data.get('position_seconds', 0))
        # Range/seek tracking
        obj.last_seek_byte = int(data.get('last_seek_byte', 0))
        obj.last_seek_percentage = float(data.get('last_seek_percentage', 0.0))
        obj.total_content_size = int(data.get('total_content_size', 0))
        obj.last_seek_timestamp = float(data.get('last_seek_timestamp', 0.0))
        return obj


class RedisBackedVODConnection:
    """Redis-backed VOD connection that can be accessed from any worker"""

    def __init__(self, session_id: str, redis_client=None):
        self.session_id = session_id
        self.redis_client = redis_client or RedisClient.get_client()
        self.connection_key = f"vod_persistent_connection:{session_id}"
        self.lock_key = f"vod_connection_lock:{session_id}"
        self.local_session = None  # Local requests session
        self.local_response = None  # Local current response

    def _get_connection_state(self) -> Optional[SerializableConnectionState]:
        """Get connection state from Redis"""
        if not self.redis_client:
            return None

        try:
            data = self.redis_client.hgetall(self.connection_key)
            if not data:
                return None

            # Convert bytes keys/values to strings if needed
            if isinstance(list(data.keys())[0], bytes):
                data = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}

            return SerializableConnectionState.from_dict(data)
        except Exception as e:
            logger.error(f"[{self.session_id}] Error getting connection state from Redis: {e}")
            return None

    def _save_connection_state(self, state: SerializableConnectionState):
        """Save connection state to Redis"""
        if not self.redis_client:
            return False

        try:
            data = state.to_dict()
            # Log the data being saved for debugging
            logger.trace(f"[{self.session_id}] Saving connection state: {data}")

            # Verify all values are valid for Redis
            for key, value in data.items():
                if value is None:
                    logger.error(f"[{self.session_id}] None value found for key '{key}' - this should not happen")
                    return False

            self.redis_client.hset(self.connection_key, mapping=data)
            self.redis_client.expire(self.connection_key, 3600)  # 1 hour TTL
            return True
        except Exception as e:
            logger.error(f"[{self.session_id}] Error saving connection state to Redis: {e}")
            return False

    def _acquire_lock(self, timeout: int = 10) -> bool:
        """Acquire distributed lock for connection operations"""
        if not self.redis_client:
            return False

        try:
            return self.redis_client.set(self.lock_key, "locked", nx=True, ex=timeout)
        except Exception as e:
            logger.error(f"[{self.session_id}] Error acquiring lock: {e}")
            return False

    def _release_lock(self):
        """Release distributed lock"""
        if not self.redis_client:
            return

        try:
            self.redis_client.delete(self.lock_key)
        except Exception as e:
            logger.error(f"[{self.session_id}] Error releasing lock: {e}")

    def create_connection(self, stream_url: str, headers: dict, m3u_profile_id: int = None,
                         # Session metadata (consolidated from vod_session key)
                         content_obj_type: str = None, content_uuid: str = None,
                         content_name: str = None, client_ip: str = None,
                         client_user_agent: str = None, utc_start: str = None,
                         utc_end: str = None, offset: str = None,
                         worker_id: str = None) -> bool:
        """Create a new connection state in Redis with consolidated session metadata"""
        if not self._acquire_lock():
            logger.warning(f"[{self.session_id}] Could not acquire lock for connection creation")
            return False

        try:
            # Check if connection already exists
            existing_state = self._get_connection_state()
            if existing_state:
                logger.info(f"[{self.session_id}] Connection already exists in Redis")
                return True

            # Create new connection state with consolidated session metadata
            state = SerializableConnectionState(
                session_id=self.session_id,
                stream_url=stream_url,
                headers=headers,
                m3u_profile_id=m3u_profile_id,
                # Session metadata
                content_obj_type=content_obj_type,
                content_uuid=content_uuid,
                content_name=content_name,
                client_ip=client_ip,
                client_user_agent=client_user_agent,
                utc_start=utc_start,
                utc_end=utc_end,
                offset=offset,
                worker_id=worker_id
            )
            success = self._save_connection_state(state)

            if success:
                logger.info(f"[{self.session_id}] Created new connection state in Redis with consolidated session metadata")

            return success
        finally:
            self._release_lock()

    def get_stream(self, range_header: str = None):
        """Get stream with optional range header - works across workers"""
        # Get connection state from Redis
        state = self._get_connection_state()
        if not state:
            logger.error(f"[{self.session_id}] No connection state found in Redis")
            return None

        # Update activity and increment request count
        state.last_activity = time.time()
        state.request_count += 1

        try:
            # Create local session if needed
            if not self.local_session:
                self.local_session = requests.Session()

            # Prepare headers
            headers = state.headers.copy()
            if range_header:
                # Validate range against content length if available
                if state.content_length:
                    validated_range = self._validate_range_header(range_header, int(state.content_length))
                    if validated_range is None:
                        logger.warning(f"[{self.session_id}] Range not satisfiable: {range_header}")
                        return None
                    range_header = validated_range

                headers['Range'] = range_header
                logger.info(f"[{self.session_id}] Setting Range header: {range_header}")

            # Use final URL if available, otherwise original URL
            target_url = state.final_url if state.final_url else state.stream_url
            allow_redirects = not state.final_url  # Only follow redirects if we don't have final URL

            logger.info(f"[{self.session_id}] Making request #{state.request_count} to {'final' if state.final_url else 'original'} URL")

            # Make request (10s connect, 10s read timeout - keeps lock time reasonable if client disconnects)
            response = self.local_session.get(
                target_url,
                headers=headers,
                stream=True,
                timeout=(10, 10),
                allow_redirects=allow_redirects
            )
            response.raise_for_status()

            # Update state with response info on first request
            if state.request_count == 1:
                if not state.content_length:
                    # Try to get full file size from Content-Range header first (for range requests)
                    content_range = response.headers.get('content-range')
                    if content_range and '/' in content_range:
                        try:
                            # Parse "bytes 0-1023/12653476926" to get total size
                            total_size = content_range.split('/')[-1]
                            if total_size.isdigit():
                                state.content_length = total_size
                                logger.debug(f"[{self.session_id}] Got full file size from Content-Range: {total_size}")
                            else:
                                # Fallback to Content-Length for partial size
                                state.content_length = response.headers.get('content-length')
                        except Exception as e:
                            logger.warning(f"[{self.session_id}] Error parsing Content-Range: {e}")
                            state.content_length = response.headers.get('content-length')
                    else:
                        # No Content-Range, use Content-Length (for non-range requests)
                        state.content_length = response.headers.get('content-length')

                logger.debug(f"[{self.session_id}] Response headers received: {dict(response.headers)}")

                if not state.content_type:  # This will be True for None, '', or any falsy value
                    # Get content type from provider response headers
                    provider_content_type = (response.headers.get('content-type') or
                                           response.headers.get('Content-Type') or
                                           response.headers.get('CONTENT-TYPE'))

                    if provider_content_type:
                        logger.debug(f"[{self.session_id}] Using provider Content-Type: '{provider_content_type}'")
                        state.content_type = provider_content_type
                    else:
                        # Provider didn't send Content-Type, infer from URL extension
                        inferred_content_type = infer_content_type_from_url(state.stream_url)
                        if inferred_content_type:
                            logger.info(f"[{self.session_id}] Provider missing Content-Type, inferred from URL: '{inferred_content_type}'")
                            state.content_type = inferred_content_type
                        else:
                            logger.debug(f"[{self.session_id}] No Content-Type from provider and could not infer from URL, using default: 'video/mp4'")
                            state.content_type = 'video/mp4'
                else:
                    logger.debug(f"[{self.session_id}] Content-Type already set in state: {state.content_type}")
                if not state.final_url:
                    state.final_url = response.url

                logger.info(f"[{self.session_id}] Updated connection state: length={state.content_length}, type={state.content_type}")

            # Save updated state
            self._save_connection_state(state)

            self.local_response = response
            return response

        except Exception as e:
            logger.error(f"[{self.session_id}] Error establishing connection: {e}")
            self.cleanup()
            raise

    def _validate_range_header(self, range_header: str, content_length: int):
        """Validate range header against content length"""
        try:
            if not range_header or not range_header.startswith('bytes='):
                return range_header

            range_part = range_header.replace('bytes=', '')
            if '-' not in range_part:
                return range_header

            start_str, end_str = range_part.split('-', 1)

            # Parse start byte
            if start_str:
                start_byte = int(start_str)
                if start_byte >= content_length:
                    return None  # Not satisfiable
            else:
                start_byte = 0

            # Parse end byte
            if end_str:
                end_byte = int(end_str)
                if end_byte >= content_length:
                    end_byte = content_length - 1
            else:
                end_byte = content_length - 1

            # Ensure start <= end
            if start_byte > end_byte:
                return None

            return f"bytes={start_byte}-{end_byte}"

        except (ValueError, IndexError) as e:
            logger.warning(f"[{self.session_id}] Could not validate range header {range_header}: {e}")
            return range_header

    def increment_active_streams(self):
        """Atomically increment active streams count in Redis"""
        if not self.redis_client:
            return False

        try:
            if not self.redis_client.exists(self.connection_key):
                logger.warning(f"[{self.session_id}] No connection state found for increment")
                return False

            pipe = self.redis_client.pipeline()
            pipe.hincrby(self.connection_key, 'active_streams', 1)
            pipe.hset(self.connection_key, 'last_activity', str(time.time()))
            new_count, _ = pipe.execute()
            logger.debug(f"[{self.session_id}] Active streams atomically incremented to {new_count}")
            return True
        except Exception as e:
            logger.error(f"[{self.session_id}] Error incrementing active streams: {e}")
            return False

    def decrement_active_streams(self):
        """Atomically decrement active streams count in Redis"""
        if not self.redis_client:
            return False

        try:
            if not self.redis_client.exists(self.connection_key):
                logger.warning(f"[{self.session_id}] No connection state found for decrement")
                return False

            pipe = self.redis_client.pipeline()
            pipe.hincrby(self.connection_key, 'active_streams', -1)
            pipe.hset(self.connection_key, 'last_activity', str(time.time()))
            new_count, _ = pipe.execute()

            if new_count < 0:
                self.redis_client.hset(self.connection_key, 'active_streams', '0')
                logger.warning(f"[{self.session_id}] active_streams went negative, reset to 0")
                new_count = 0

            logger.debug(f"[{self.session_id}] Active streams atomically decremented to {new_count}")
            return True
        except Exception as e:
            logger.error(f"[{self.session_id}] Error decrementing active streams: {e}")
            return False

    def has_active_streams(self) -> bool:
        """Check if connection has any active streams"""
        state = self._get_connection_state()
        return state.active_streams > 0 if state else False

    def get_headers(self):
        """Get headers for response"""
        state = self._get_connection_state()
        if state:
            return {
                'content_length': state.content_length,
                'content_type': state.content_type or 'video/mp4',
                'final_url': state.final_url
            }
        return {}

    def get_session_metadata(self):
        """Get session metadata from consolidated connection state"""
        state = self._get_connection_state()
        if state:
            return {
                'content_obj_type': state.content_obj_type,
                'content_uuid': state.content_uuid,
                'content_name': state.content_name,
                'client_ip': state.client_ip,
                'client_user_agent': state.client_user_agent,
                'utc_start': state.utc_start,
                'utc_end': state.utc_end,
                'offset': state.offset,
                'worker_id': state.worker_id,
                'connection_type': state.connection_type,
                'created_at': state.created_at,
                'last_activity': state.last_activity,
                'm3u_profile_id': state.m3u_profile_id,
                'bytes_sent': state.bytes_sent,
                'position_seconds': state.position_seconds,
                'active_streams': state.active_streams,
                'request_count': state.request_count,
                # Range/seek tracking
                'last_seek_byte': state.last_seek_byte,
                'last_seek_percentage': state.last_seek_percentage,
                'total_content_size': state.total_content_size,
                'last_seek_timestamp': state.last_seek_timestamp
            }
        return {}

    def cleanup(self, connection_manager=None, current_worker_id=None, force=False):
        """Smart cleanup based on worker ownership and active streams
        
        Args:
            connection_manager: The connection manager instance for decrementing profile connections
            current_worker_id: The current worker ID for ownership checks
            force: If True, skip active_streams check and force cleanup even if streams are active
        """
        # Always clean up local resources first
        if self.local_response:
            self.local_response.close()
            self.local_response = None
        if self.local_session:
            self.local_session.close()
            self.local_session = None

        # Get current connection state to check ownership and active streams
        state = self._get_connection_state()

        if not state:
            logger.info(f"[{self.session_id}] No connection state found - local cleanup only")
            return

        # Check if there are active streams (skip if force=True)
        if not force and state.active_streams > 0:
            # There are active streams - check ownership
            if current_worker_id and state.worker_id == current_worker_id:
                logger.info(f"[{self.session_id}] Active streams present ({state.active_streams}) and we own them - local cleanup only")
            else:
                logger.info(f"[{self.session_id}] Active streams present ({state.active_streams}) but owned by worker {state.worker_id} - local cleanup only")
            return

        # Force cleanup or no active streams - we can clean up Redis state
        if force:
            logger.info(f"[{self.session_id}] Force cleanup requested - proceeding despite active_streams={state.active_streams}")
        
        if not self.redis_client:
            logger.info(f"[{self.session_id}] No Redis client - local cleanup only")
            return

        # Acquire lock and do final check before cleanup to prevent race conditions
        if not self._acquire_lock():
            logger.warning(f"[{self.session_id}] Could not acquire lock for cleanup - skipping")
            return

        try:
            # Re-check active streams with lock held to prevent race conditions (skip if force=True)
            current_state = self._get_connection_state()
            if not current_state:
                logger.info(f"[{self.session_id}] Connection state no longer exists - cleanup already done")
                return

            # Safety net: Reset stuck active_streams counter if last_activity is stale
            # This handles cases where counters are stuck due to previous bugs or race conditions
            if current_state.active_streams > 0:
                current_time = time.time()
                time_since_activity = current_time - current_state.last_activity
                stale_threshold = 10  # Reduced from 30s to 10s for faster detection of stale connections
                
                if time_since_activity > stale_threshold:
                    logger.warning(
                        f"[{self.session_id}] Safety net: active_streams={current_state.active_streams} but "
                        f"last_activity is {time_since_activity:.1f}s old (>{stale_threshold}s). "
                        f"Resetting active_streams to 0."
                    )
                    current_state.active_streams = 0
                    self._save_connection_state(current_state)
                    # Update current_state reference for logging below
                    current_state = self._get_connection_state()

            if not force and current_state.active_streams > 0:
                logger.info(f"[{self.session_id}] Active streams now present ({current_state.active_streams}) - skipping cleanup")
                return

            # Use pipeline for atomic cleanup operations
            pipe = self.redis_client.pipeline()

            # 1. Remove main connection state (contains consolidated data)
            pipe.delete(self.connection_key)

            # 2. Remove distributed lock (will be released below anyway)
            pipe.delete(self.lock_key)

            # Execute all cleanup operations
            pipe.execute()

            if force:
                logger.info(f"[{self.session_id}] Force cleaned up Redis keys (active_streams={current_state.active_streams})")
            else:
                logger.info(f"[{self.session_id}] Cleaned up Redis keys (verified no active streams)")

            # Decrement profile connections if we have the state and connection manager
            if state.m3u_profile_id and connection_manager:
                connection_manager._decrement_profile_connections(state.m3u_profile_id)
                logger.info(f"[{self.session_id}] Profile connection count decremented for profile {state.m3u_profile_id}")
            else:
                if not state.m3u_profile_id:
                    logger.warning(f"[{self.session_id}] No profile ID in connection state - cannot decrement profile connections")
                elif not connection_manager:
                    logger.warning(f"[{self.session_id}] No connection manager provided - cannot decrement profile connections")

        except Exception as e:
            logger.error(f"[{self.session_id}] Error cleaning up Redis state: {e}")
        finally:
            # Always release the lock
            self._release_lock()


# Modify the VODConnectionManager to use Redis-backed connections
class MultiWorkerVODConnectionManager:
    """Enhanced VOD Connection Manager that works across multiple uwsgi workers"""

    _instance = None

    @classmethod
    def get_instance(cls):
        """Get the singleton instance"""
        if cls._instance is None:
            cls._instance = cls()
        return cls._instance

    def __init__(self):
        self.redis_client = RedisClient.get_client()
        self.connection_ttl = 3600  # 1 hour TTL for connections
        self.session_ttl = 1800  # 30 minutes TTL for sessions
        self.worker_id = self._get_worker_id()
        logger.info(f"MultiWorkerVODConnectionManager initialized for worker {self.worker_id}")

    def _normalize_user_agent_for_session(self, user_agent: str) -> str:
        """
        Normalize user agent for session matching.
        Groups Jellyfin-related user agents (Lavf/, ffmpeg, ffprobe) together.
        
        Args:
            user_agent: Original user agent string
            
        Returns:
            Normalized user agent string for session matching
        """
        if not user_agent:
            return "unknown"
        
        user_agent_lower = user_agent.lower()
        
        # Group Jellyfin-related user agents
        if any(keyword in user_agent_lower for keyword in ['lavf/', 'ffmpeg', 'ffprobe', 'jellyfin']):
            return "jellyfin_client"
        
        # Keep other user agents as-is for now (VLC, etc.)
        return user_agent

    def _get_worker_id(self):
        """Get unique worker ID for this process"""
        import os
        import socket
        try:
            # Use combination of hostname and PID for unique worker ID
            return f"{socket.gethostname()}-{os.getpid()}"
        except:
            import random
            return f"worker-{random.randint(1000, 9999)}"

    def _get_profile_connections_key(self, profile_id: int) -> str:
        """Get Redis key for tracking connections per profile - STANDARDIZED with TS proxy"""
        return f"profile_connections:{profile_id}"

    def _check_profile_limits(self, m3u_profile) -> bool:
        """Check if profile has available connection slots"""
        if m3u_profile.max_streams == 0:  # Unlimited
            return True

        try:
            profile_connections_key = self._get_profile_connections_key(m3u_profile.id)
            current_connections = int(self.redis_client.get(profile_connections_key) or 0)

            logger.info(f"[PROFILE-CHECK] Profile {m3u_profile.id} has {current_connections}/{m3u_profile.max_streams} connections")
            return current_connections < m3u_profile.max_streams

        except Exception as e:
            logger.error(f"Error checking profile limits: {e}")
            return False

    def _increment_profile_connections(self, m3u_profile):
        """Increment profile connection count"""
        try:
            profile_connections_key = self._get_profile_connections_key(m3u_profile.id)
            new_count = self.redis_client.incr(profile_connections_key)
            logger.info(f"[PROFILE-INCR] Profile {m3u_profile.id} connections: {new_count}")
            return new_count
        except Exception as e:
            logger.error(f"Error incrementing profile connections: {e}")
            return None

    def _decrement_profile_connections(self, m3u_profile_id: int):
        """Decrement profile connection count"""
        try:
            profile_connections_key = self._get_profile_connections_key(m3u_profile_id)
            current_count = int(self.redis_client.get(profile_connections_key) or 0)
            if current_count > 0:
                new_count = self.redis_client.decr(profile_connections_key)
                logger.info(f"[PROFILE-DECR] Profile {m3u_profile_id} connections: {new_count}")
                return new_count
            else:
                logger.warning(f"[PROFILE-DECR] Profile {m3u_profile_id} already at 0 connections")
                return 0
        except Exception as e:
            logger.error(f"Error decrementing profile connections: {e}")
            return None

    def _schedule_delayed_cleanup(self, redis_connection, client_id: str, reason: str):
        """Schedule delayed cleanup with retry logic when no active streams."""
        def delayed_cleanup():
            max_retries = 3
            retry_delay = 0.2  # Reduced from 0.5s to 0.2s for faster cleanup

            for attempt in range(max_retries):
                time.sleep(retry_delay)

                # Check active streams before cleanup
                if redis_connection.has_active_streams():
                    logger.info(
                        f"[{client_id}] Worker {self.worker_id} - Active streams still present "
                        f"({attempt + 1}/{max_retries}), retrying cleanup"
                    )
                    if attempt < max_retries - 1:
                        continue
                    else:
                        # Last attempt - force cleanup
                        logger.info(
                            f"[{client_id}] Worker {self.worker_id} - Forcing cleanup after "
                            f"{max_retries} retries"
                        )
                        redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id, force=True)
                        return

                # Try normal cleanup
                logger.info(
                    f"[{client_id}] Worker {self.worker_id} - Attempting cleanup after {reason} "
                    f"(attempt {attempt + 1}/{max_retries})"
                )
                redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id)

                # Check if cleanup succeeded (connection state should be gone)
                if not redis_connection._get_connection_state():
                    logger.info(f"[{client_id}] Worker {self.worker_id} - Cleanup succeeded")
                    return

                logger.debug(
                    f"[{client_id}] Worker {self.worker_id} - Cleanup did not complete, will retry"
                )

            # If we get here, cleanup didn't succeed after all retries
            # Force cleanup as last resort
            logger.warning(
                f"[{client_id}] Worker {self.worker_id} - Cleanup did not succeed after "
                f"{max_retries} retries, forcing cleanup"
            )
            redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id, force=True)

        cleanup_thread = threading.Thread(target=delayed_cleanup, daemon=True)
        cleanup_thread.start()

    def _schedule_safety_net_cleanup(self, redis_connection, client_id: str,
                                     delay_seconds: float = 5.0, staleness_threshold: float = 5.0):
        """Schedule a safety-net cleanup if active_streams looks stuck."""
        def safety_cleanup():
            time.sleep(delay_seconds)
            state = redis_connection._get_connection_state()
            if state and state.active_streams > 0:
                staleness = time.time() - state.last_activity
                if staleness > staleness_threshold:
                    logger.warning(
                        f"[{client_id}] Safety net: counter stuck at {state.active_streams}, "
                        f"last_activity {staleness:.1f}s old, forcing cleanup"
                    )
                    redis_connection.cleanup(
                        connection_manager=self,
                        current_worker_id=self.worker_id,
                        force=True
                    )

        cleanup_thread = threading.Thread(target=safety_cleanup, daemon=True)
        cleanup_thread.start()

    def stream_content_with_session(self, session_id, content_obj, stream_url, m3u_profile,
                                  client_ip, client_user_agent, request,
                                  utc_start=None, utc_end=None, offset=None, range_header=None):
        """Stream content with Redis-backed persistent connection"""

        # Generate client ID
        content_type = "movie" if isinstance(content_obj, Movie) else "episode"
        content_uuid = str(content_obj.uuid)
        content_name = content_obj.name if hasattr(content_obj, 'name') else str(content_obj)
        client_id = session_id

        # Track whether we incremented profile connections (for cleanup on error)
        profile_connections_incremented = False
        redis_connection = None

        logger.info(f"[{client_id}] Worker {self.worker_id} - Redis-backed streaming request for {content_type} {content_name}")

        try:
            # First, try to find an existing session that matches our criteria
            # First try with allow_active_streams=True to catch Jellyfin's rapid requests
            matching_session_id = self.find_matching_idle_session(
                content_type=content_type,
                content_uuid=content_uuid,
                client_ip=client_ip,
                client_user_agent=client_user_agent,
                utc_start=utc_start,
                utc_end=utc_end,
                offset=offset,
                allow_active_streams=True  # Allow matching sessions with active streams for same client+content
            )
            
            # If no match with active streams allowed, try again for idle sessions only
            if not matching_session_id:
                matching_session_id = self.find_matching_idle_session(
                    content_type=content_type,
                    content_uuid=content_uuid,
                    client_ip=client_ip,
                    client_user_agent=client_user_agent,
                    utc_start=utc_start,
                    utc_end=utc_end,
                    offset=offset,
                    allow_active_streams=False  # Only idle sessions
                )

            # Use matching session if found, otherwise use the provided session_id
            if matching_session_id:
                logger.info(f"[{client_id}] Worker {self.worker_id} - Found matching idle session: {matching_session_id}")
                effective_session_id = matching_session_id
                client_id = matching_session_id  # Update client_id for logging consistency
                # Note: We don't increment active_streams here - it will be done inside the generator
                # to ensure it's always paired with a decrement in the finally block
            else:
                logger.info(f"[{client_id}] Worker {self.worker_id} - No matching idle session found, using new session")
                effective_session_id = session_id

            # Create Redis-backed connection
            redis_connection = RedisBackedVODConnection(effective_session_id, self.redis_client)

            # Check if connection exists, create if not
            existing_state = redis_connection._get_connection_state()
            if not existing_state:
                logger.info(f"[{client_id}] Worker {self.worker_id} - Creating new Redis-backed connection")

                # Check profile limits before creating new connection
                if not self._check_profile_limits(m3u_profile):
                    logger.warning(f"[{client_id}] Profile {m3u_profile.name} connection limit exceeded")
                    return HttpResponse("Connection limit exceeded for profile", status=429)

                # Apply timeshift parameters
                modified_stream_url = self._apply_timeshift_parameters(stream_url, utc_start, utc_end, offset)

                # Prepare headers for provider request
                headers = {}
                # Use M3U account's user-agent for provider requests, not client's user-agent
                m3u_user_agent = m3u_profile.m3u_account.get_user_agent()
                if m3u_user_agent:
                    headers['User-Agent'] = m3u_user_agent.user_agent
                    logger.info(f"[{client_id}] Using M3U account user-agent: {m3u_user_agent.user_agent}")
                elif client_user_agent:
                    # Fallback to client's user-agent if M3U doesn't have one
                    headers['User-Agent'] = client_user_agent
                    logger.info(f"[{client_id}] Using client user-agent (M3U fallback): {client_user_agent}")
                else:
                    logger.warning(f"[{client_id}] No user-agent available (neither M3U nor client)")

                # Forward important headers from request
                important_headers = ['authorization', 'referer', 'origin', 'accept']
                for header_name in important_headers:
                    django_header = f'HTTP_{header_name.upper().replace("-", "_")}'
                    if hasattr(request, 'META') and django_header in request.META:
                        headers[header_name] = request.META[django_header]

                # Create connection state in Redis with consolidated session metadata
                if not redis_connection.create_connection(
                    stream_url=modified_stream_url,
                    headers=headers,
                    m3u_profile_id=m3u_profile.id,
                    # Session metadata (consolidated from separate vod_session key)
                    content_obj_type=content_type,
                    content_uuid=content_uuid,
                    content_name=content_name,
                    client_ip=client_ip,
                    client_user_agent=client_user_agent,
                    utc_start=utc_start,
                    utc_end=utc_end,
                    offset=str(offset) if offset else None,
                    worker_id=self.worker_id
                ):
                    logger.error(f"[{client_id}] Worker {self.worker_id} - Failed to create Redis connection")
                    return HttpResponse("Failed to create connection", status=500)

                # Increment profile connections after successful connection creation
                self._increment_profile_connections(m3u_profile)
                profile_connections_incremented = True

                logger.info(f"[{client_id}] Worker {self.worker_id} - Created consolidated connection with session metadata")
            else:
                logger.info(f"[{client_id}] Worker {self.worker_id} - Using existing Redis-backed connection")

                # Transfer ownership to current worker and update session activity
                if redis_connection._acquire_lock():
                    try:
                        state = redis_connection._get_connection_state()
                        if state:
                            old_worker = state.worker_id
                            state.last_activity = time.time()
                            state.worker_id = self.worker_id  # Transfer ownership to current worker
                            redis_connection._save_connection_state(state)

                            if old_worker != self.worker_id:
                                logger.info(f"[{client_id}] Ownership transferred from worker {old_worker} to {self.worker_id}")
                            else:
                                logger.debug(f"[{client_id}] Worker {self.worker_id} retaining ownership")
                    finally:
                        redis_connection._release_lock()

            # Get stream from Redis-backed connection
            upstream_response = redis_connection.get_stream(range_header)

            if upstream_response is None:
                logger.warning(f"[{client_id}] Worker {self.worker_id} - Range not satisfiable")
                return HttpResponse("Requested Range Not Satisfiable", status=416)

            # Get connection headers
            connection_headers = redis_connection.get_headers()

            # Create streaming generator
            def stream_generator():
                decremented = False
                stop_signal_detected = False
                try:
                    logger.info(f"[{client_id}] Worker {self.worker_id} - Starting Redis-backed stream")

                    # Increment active streams for ALL sessions (both new and reused)
                    # This ensures increment and decrement are in the same scope (the generator),
                    # guaranteeing cleanup even if the request fails before streaming starts
                    redis_connection.increment_active_streams()
                    if matching_session_id:
                        logger.debug(f"[{client_id}] Incremented active streams for reused session")
                    else:
                        logger.debug(f"[{client_id}] Incremented active streams for new session")

                    bytes_sent = 0
                    chunk_count = 0

                    # Get the stop signal key for this client
                    stop_key = get_vod_client_stop_key(client_id)

                    for chunk in upstream_response.iter_content(chunk_size=8192):
                        if chunk:
                            yield chunk
                            bytes_sent += len(chunk)
                            chunk_count += 1

                            # Check for stop signal every 100 chunks
                            if chunk_count % 100 == 0:
                                # Check if stop signal has been set
                                if self.redis_client and self.redis_client.exists(stop_key):
                                    logger.info(f"[{client_id}] Worker {self.worker_id} - Stop signal detected, terminating stream")
                                    # Delete the stop key
                                    self.redis_client.delete(stop_key)
                                    stop_signal_detected = True
                                    break

                                # Update the connection state
                                logger.debug(f"Client: [{client_id}] Worker: {self.worker_id} sent {chunk_count} chunks for VOD: {content_name}")
                                if redis_connection._acquire_lock():
                                    try:
                                        state = redis_connection._get_connection_state()
                                        if state:
                                            state.last_activity = time.time()
                                            # Store cumulative bytes sent in connection state
                                            state.bytes_sent = bytes_sent  # Use cumulative bytes_sent, not chunk size
                                            redis_connection._save_connection_state(state)
                                    finally:
                                        redis_connection._release_lock()

                    if stop_signal_detected:
                        logger.info(f"[{client_id}] Worker {self.worker_id} - Stream stopped by signal: {bytes_sent} bytes sent")
                        decrement_result = redis_connection.decrement_active_streams()
                        if decrement_result:
                            decremented = True
                        # Force immediate cleanup when stop signal is detected
                        logger.info(f"[{client_id}] Worker {self.worker_id} - Forcing immediate cleanup after stop signal")
                        redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id, force=True)
                    else:
                        logger.info(f"[{client_id}] Worker {self.worker_id} - Redis-backed stream completed: {bytes_sent} bytes sent")
                        decrement_result = redis_connection.decrement_active_streams()
                        if decrement_result:
                            decremented = True

                        # Schedule smart cleanup if no active streams after normal completion
                        if not redis_connection.has_active_streams():
                            self._schedule_delayed_cleanup(
                                redis_connection,
                                client_id,
                                reason="normal completion"
                            )
                        else:
                            self._schedule_safety_net_cleanup(redis_connection, client_id)

                except (OSError, BrokenPipeError, ConnectionResetError) as e:
                    # Handle broken pipe and connection reset errors during yield
                    # These occur when client disconnects during streaming
                    logger.info(f"[{client_id}] Worker {self.worker_id} - Client disconnected (broken pipe/connection reset): {type(e).__name__}")
                    decrement_result = None
                    if not decremented:
                        # Try to decrement - even if it returns False (already at 0), we still need to check for cleanup
                        decrement_result = redis_connection.decrement_active_streams()
                        if not decrement_result:
                            logger.info(f"[{client_id}] Worker {self.worker_id} - Active streams already at 0 (race condition detected)")
                        else:
                            decremented = True

                    # Always check for cleanup after disconnect, even if decrement returned False
                    # This handles the case where multiple workers disconnect simultaneously
                    # If decrement returned False (race condition), be more aggressive with cleanup
                    if not redis_connection.has_active_streams():
                        # If we detected a race condition (decrement returned False), force cleanup immediately
                        if decrement_result is False:
                            logger.info(f"[{client_id}] Worker {self.worker_id} - Race condition detected, forcing immediate cleanup")
                            redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id, force=True)
                        else:
                            self._schedule_delayed_cleanup(
                                redis_connection,
                                client_id,
                                reason="client disconnect"
                            )
                    else:
                        # Even if active streams are present, log this for debugging
                        state = redis_connection._get_connection_state()
                        active_count = state.active_streams if state else 'unknown'
                        logger.debug(f"[{client_id}] Worker {self.worker_id} - Active streams still present ({active_count}), scheduling safety-net cleanup")
                        self._schedule_safety_net_cleanup(redis_connection, client_id)

                except GeneratorExit:
                    logger.info(f"[{client_id}] Worker {self.worker_id} - Client disconnected from Redis-backed stream (generator exit)")
                    decrement_result = None
                    if not decremented:
                        # Try to decrement - even if it returns False (already at 0), we still need to check for cleanup
                        decrement_result = redis_connection.decrement_active_streams()
                        if not decrement_result:
                            logger.info(f"[{client_id}] Worker {self.worker_id} - Active streams already at 0 (race condition detected)")
                        else:
                            decremented = True

                    # Always check for cleanup after disconnect, even if decrement returned False
                    # This handles the case where multiple workers disconnect simultaneously
                    # If decrement returned False (race condition), be more aggressive with cleanup
                    if not redis_connection.has_active_streams():
                        # If we detected a race condition (decrement returned False), force cleanup immediately
                        if decrement_result is False:
                            logger.info(f"[{client_id}] Worker {self.worker_id} - Race condition detected, forcing immediate cleanup")
                            redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id, force=True)
                        else:
                            self._schedule_delayed_cleanup(
                                redis_connection,
                                client_id,
                                reason="client disconnect"
                            )
                    else:
                        # Even if active streams are present, log this for debugging
                        state = redis_connection._get_connection_state()
                        active_count = state.active_streams if state else 'unknown'
                        logger.debug(f"[{client_id}] Worker {self.worker_id} - Active streams still present ({active_count}), scheduling safety-net cleanup")
                        self._schedule_safety_net_cleanup(redis_connection, client_id)

                except Exception as e:
                    logger.error(f"[{client_id}] Worker {self.worker_id} - Error in Redis-backed stream: {e}")
                    if not decremented:
                        redis_connection.decrement_active_streams()
                        decremented = True
                    # Smart cleanup on error - immediate cleanup since we're in error state
                    redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id)
                    yield b"Error: Stream interrupted"

                finally:
                    # Always decrement active streams to ensure cleanup, even if exception occurred
                    # This is the safety net that guarantees every increment is matched with a decrement
                    if not decremented:
                        logger.debug(f"[{client_id}] Worker {self.worker_id} - Finally block: decrementing active streams (was not decremented in exception handler)")
                        redis_connection.decrement_active_streams()
                        decremented = True
                    else:
                        logger.debug(f"[{client_id}] Worker {self.worker_id} - Finally block: active streams already decremented")

            # Create streaming response
            response = StreamingHttpResponse(
                streaming_content=stream_generator(),
                content_type=connection_headers.get('content_type', 'video/mp4')
            )

            # Set appropriate status code
            response.status_code = 206 if range_header else 200

            # Set required headers
            response['Cache-Control'] = 'no-cache'
            response['Pragma'] = 'no-cache'
            response['X-Content-Type-Options'] = 'nosniff'
            response['Connection'] = 'keep-alive'
            response['X-Worker-ID'] = self.worker_id  # Identify which worker served this

            if connection_headers.get('content_length'):
                response['Accept-Ranges'] = 'bytes'

                # For range requests, Content-Length should be the partial content size, not full file size
                if range_header and 'bytes=' in range_header:
                    try:
                        range_part = range_header.replace('bytes=', '')
                        if '-' in range_part:
                            start_byte, end_byte = range_part.split('-', 1)
                            start = int(start_byte) if start_byte else 0

                            # Get the FULL content size from the connection state (from initial request)
                            state = redis_connection._get_connection_state()
                            if state and state.content_length:
                                full_content_size = int(state.content_length)
                                end = int(end_byte) if end_byte else full_content_size - 1

                                # Calculate partial content size for Content-Length header
                                partial_content_size = end - start + 1
                                response['Content-Length'] = str(partial_content_size)

                                # Content-Range should show full file size per HTTP standards
                                content_range = f"bytes {start}-{end}/{full_content_size}"
                                response['Content-Range'] = content_range
                                logger.info(f"[{client_id}] Worker {self.worker_id} - Set Content-Range: {content_range}, Content-Length: {partial_content_size}")

                                # Store range information for the VOD stats API to calculate position
                                if start > 0:
                                    try:
                                        position_percentage = (start / full_content_size) * 100
                                        current_timestamp = time.time()

                                        # Update the Redis connection state with seek information
                                        if redis_connection._acquire_lock():
                                            try:
                                                # Refresh state in case it changed
                                                state = redis_connection._get_connection_state()
                                                if state:
                                                    # Store range/seek information for stats API
                                                    state.last_seek_byte = start
                                                    state.last_seek_percentage = position_percentage
                                                    state.total_content_size = full_content_size
                                                    state.last_seek_timestamp = current_timestamp
                                                    state.last_activity = current_timestamp
                                                    redis_connection._save_connection_state(state)
                                                    logger.info(f"[{client_id}] *** SEEK INFO STORED *** {position_percentage:.1f}% at byte {start:,}/{full_content_size:,} (timestamp: {current_timestamp})")
                                            finally:
                                                redis_connection._release_lock()
                                        else:
                                            logger.warning(f"[{client_id}] Could not acquire lock to update seek info")
                                    except Exception as pos_e:
                                        logger.error(f"[{client_id}] Error storing seek info: {pos_e}")
                            else:
                                # Fallback to partial content size if full size not available
                                partial_size = int(connection_headers['content_length'])
                                end = int(end_byte) if end_byte else partial_size - 1
                                content_range = f"bytes {start}-{end}/{partial_size}"
                                response['Content-Range'] = content_range
                                response['Content-Length'] = str(end - start + 1)
                                logger.warning(f"[{client_id}] Using partial content size for Content-Range (full size not available): {content_range}")
                    except Exception as e:
                        logger.warning(f"[{client_id}] Worker {self.worker_id} - Could not set Content-Range: {e}")
                        response['Content-Length'] = connection_headers['content_length']
                else:
                    # For non-range requests, use the full content length
                    response['Content-Length'] = connection_headers['content_length']

            logger.info(f"[{client_id}] Worker {self.worker_id} - Redis-backed response ready (status: {response.status_code})")
            return response

        except Exception as e:
            logger.error(f"[{client_id}] Worker {self.worker_id} - Error in Redis-backed stream_content_with_session: {e}", exc_info=True)

            # Decrement profile connections if we incremented them but failed before streaming started
            if profile_connections_incremented:
                logger.info(f"[{client_id}] Connection error occurred after profile increment - decrementing profile connections")
                self._decrement_profile_connections(m3u_profile.id)

                # Also clean up the Redis connection state since we won't be using it
                if redis_connection:
                    try:
                        redis_connection.cleanup(connection_manager=self, current_worker_id=self.worker_id)
                    except Exception as cleanup_error:
                        logger.error(f"[{client_id}] Error during cleanup after connection failure: {cleanup_error}")

            return HttpResponse(f"Streaming error: {str(e)}", status=500)

    def _apply_timeshift_parameters(self, original_url, utc_start=None, utc_end=None, offset=None):
        """Apply timeshift parameters to URL"""
        if not any([utc_start, utc_end, offset]):
            return original_url

        try:
            from urllib.parse import urlparse, urlunparse, parse_qs, urlencode

            parsed_url = urlparse(original_url)
            query_params = parse_qs(parsed_url.query)
            path = parsed_url.path

            logger.info(f"Applying timeshift parameters: utc_start={utc_start}, utc_end={utc_end}, offset={offset}")

            # Add timeshift parameters
            if utc_start:
                query_params['utc_start'] = [utc_start]
                query_params['start'] = [utc_start]
                logger.info(f"Added utc_start/start parameter: {utc_start}")

            if utc_end:
                query_params['utc_end'] = [utc_end]
                query_params['end'] = [utc_end]
                logger.info(f"Added utc_end/end parameter: {utc_end}")

            if offset:
                try:
                    offset_seconds = int(offset)
                    query_params['offset'] = [str(offset_seconds)]
                    query_params['seek'] = [str(offset_seconds)]
                    query_params['t'] = [str(offset_seconds)]
                    logger.info(f"Added offset/seek/t parameter: {offset_seconds}")
                except ValueError:
                    logger.warning(f"Invalid offset value: {offset}")

            # Handle special catchup URL patterns
            if utc_start:
                try:
                    from datetime import datetime
                    import re

                    # Parse the UTC start time
                    start_dt = datetime.fromisoformat(utc_start.replace('Z', '+00:00'))

                    # Check for catchup URL patterns like /catchup/YYYY-MM-DD/HH-MM-SS/
                    catchup_pattern = r'/catchup/\d{4}-\d{2}-\d{2}/\d{2}-\d{2}-\d{2}/'
                    if re.search(catchup_pattern, path):
                        # Replace the date/time in the path
                        date_part = start_dt.strftime('%Y-%m-%d')
                        time_part = start_dt.strftime('%H-%M-%S')

                        path = re.sub(catchup_pattern, f'/catchup/{date_part}/{time_part}/', path)
                        logger.info(f"Modified catchup path: {path}")
                except Exception as e:
                    logger.warning(f"Could not parse timeshift date: {e}")

            # Reconstruct URL
            new_query = urlencode(query_params, doseq=True)
            modified_url = urlunparse((
                parsed_url.scheme,
                parsed_url.netloc,
                path,
                parsed_url.params,
                new_query,
                parsed_url.fragment
            ))

            logger.info(f"Modified URL: {modified_url}")
            return modified_url

        except Exception as e:
            logger.error(f"Error applying timeshift parameters: {e}")
            return original_url

    def cleanup_persistent_connection(self, session_id: str, force=False):
        """Clean up a specific Redis-backed persistent connection
        
        Args:
            session_id: The session ID to clean up
            force: If True, force cleanup even if active streams are present
        """
        logger.info(f"[{session_id}] Cleaning up Redis-backed persistent connection (force={force})")

        redis_connection = RedisBackedVODConnection(session_id, self.redis_client)
        redis_connection.cleanup(connection_manager=self, force=force)

        # The cleanup method now handles all Redis keys including session data

    def cleanup_stale_persistent_connections(self, max_age_seconds: int = 1800):
        """Clean up stale Redis-backed persistent connections"""
        if not self.redis_client:
            return

        try:
            logger.info(f"Cleaning up Redis-backed connections older than {max_age_seconds} seconds")

            # Find all persistent connection keys
            pattern = "vod_persistent_connection:*"
            cursor = 0
            cleanup_count = 0
            current_time = time.time()

            while True:
                cursor, keys = self.redis_client.scan(cursor, match=pattern, count=100)

                for key in keys:
                    try:
                        # Get connection state
                        data = self.redis_client.hgetall(key)
                        if not data:
                            continue

                        # Convert bytes to strings if needed
                        if isinstance(list(data.keys())[0], bytes):
                            data = {k.decode('utf-8'): v.decode('utf-8') for k, v in data.items()}

                        last_activity = float(data.get('last_activity', 0))
                        active_streams = int(data.get('active_streams', 0))

                        # Clean up if stale and no active streams
                        if (current_time - last_activity > max_age_seconds) and active_streams == 0:
                            session_id = key.decode('utf-8').replace('vod_persistent_connection:', '')
                            logger.info(f"Cleaning up stale connection: {session_id}")

                            # Clean up connection and related keys
                            redis_connection = RedisBackedVODConnection(session_id, self.redis_client)
                            redis_connection.cleanup(connection_manager=self)
                            cleanup_count += 1

                    except Exception as e:
                        logger.error(f"Error processing connection key {key}: {e}")
                        continue

                if cursor == 0:
                    break

            if cleanup_count > 0:
                logger.info(f"Cleaned up {cleanup_count} stale Redis-backed connections")
            else:
                logger.debug("No stale Redis-backed connections found")

        except Exception as e:
            logger.error(f"Error during Redis-backed connection cleanup: {e}")

    def create_connection(self, content_type: str, content_uuid: str, content_name: str,
                         client_id: str, client_ip: str, user_agent: str,
                         m3u_profile: M3UAccountProfile) -> bool:
        """Create connection tracking in Redis (same as original but for Redis-backed connections)"""
        if not self.redis_client:
            logger.error("Redis client not available for VOD connection tracking")
            return False

        try:
            # Check profile connection limits
            profile_connections_key = f"profile_connections:{m3u_profile.id}"
            current_connections = self.redis_client.get(profile_connections_key)
            max_connections = getattr(m3u_profile, 'max_connections', 3)  # Default to 3

            if current_connections and int(current_connections) >= max_connections:
                logger.warning(f"Profile {m3u_profile.name} connection limit exceeded ({current_connections}/{max_connections})")
                return False

            # Create connection tracking
            connection_key = f"vod_proxy:connection:{content_type}:{content_uuid}:{client_id}"
            content_connections_key = f"vod_proxy:content:{content_type}:{content_uuid}:connections"

            # Check if connection already exists
            if self.redis_client.exists(connection_key):
                logger.info(f"Connection already exists for {client_id} - {content_type} {content_name}")
                self.redis_client.hset(connection_key, "last_activity", str(time.time()))
                return True

            # Connection data
            connection_data = {
                "content_type": content_type,
                "content_uuid": content_uuid,
                "content_name": content_name,
                "client_id": client_id,
                "client_ip": client_ip,
                "user_agent": user_agent,
                "m3u_profile_id": m3u_profile.id,
                "m3u_profile_name": m3u_profile.name,
                "connected_at": str(time.time()),
                "last_activity": str(time.time()),
                "bytes_sent": "0",
                "position_seconds": "0"
            }

            # Use pipeline for atomic operations
            pipe = self.redis_client.pipeline()
            pipe.hset(connection_key, mapping=connection_data)
            pipe.expire(connection_key, self.connection_ttl)
            pipe.incr(profile_connections_key)
            pipe.sadd(content_connections_key, client_id)
            pipe.expire(content_connections_key, self.connection_ttl)
            pipe.execute()

            logger.info(f"Created Redis-backed VOD connection: {client_id} for {content_type} {content_name}")
            return True

        except Exception as e:
            logger.error(f"Error creating Redis-backed connection: {e}")
            return False

    def remove_connection(self, content_type: str, content_uuid: str, client_id: str):
        """Remove connection tracking from Redis"""
        if not self.redis_client:
            return

        try:
            connection_key = f"vod_proxy:connection:{content_type}:{content_uuid}:{client_id}"
            content_connections_key = f"vod_proxy:content:{content_type}:{content_uuid}:connections"

            # Get connection data to find profile
            connection_data = self.redis_client.hgetall(connection_key)
            if connection_data:
                # Convert bytes to strings if needed
                if isinstance(list(connection_data.keys())[0], bytes):
                    connection_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in connection_data.items()}

                profile_id = connection_data.get('m3u_profile_id')
                if profile_id:
                    profile_connections_key = f"profile_connections:{profile_id}"

                    # Use pipeline for atomic operations
                    pipe = self.redis_client.pipeline()
                    pipe.delete(connection_key)
                    pipe.srem(content_connections_key, client_id)
                    pipe.decr(profile_connections_key)
                    pipe.execute()

                    logger.info(f"Removed Redis-backed connection: {client_id}")

        except Exception as e:
            logger.error(f"Error removing Redis-backed connection: {e}")

    def update_connection_activity(self, content_type: str, content_uuid: str,
                                 client_id: str, bytes_sent: int):
        """Update connection activity in Redis"""
        if not self.redis_client:
            return

        try:
            connection_key = f"vod_proxy:connection:{content_type}:{content_uuid}:{client_id}"
            pipe = self.redis_client.pipeline()
            pipe.hset(connection_key, mapping={
                "last_activity": str(time.time()),
                "bytes_sent": str(bytes_sent)
            })
            pipe.expire(connection_key, self.connection_ttl)
            pipe.execute()
        except Exception as e:
            logger.error(f"Error updating connection activity: {e}")

    def find_matching_idle_session(self, content_type: str, content_uuid: str,
                                 client_ip: str, client_user_agent: str,
                                 utc_start=None, utc_end=None, offset=None,
                                 allow_active_streams=False) -> Optional[str]:
        """
        Find existing Redis-backed session that matches criteria using consolidated connection state.
        
        Args:
            content_type: Type of content (movie, episode, series)
            content_uuid: UUID of the content
            client_ip: Client IP address
            client_user_agent: Client user agent
            utc_start: UTC start time for timeshift
            utc_end: UTC end time for timeshift
            offset: Offset in seconds
            allow_active_streams: If True, match sessions even if they have active streams
                                 (useful for Jellyfin's rapid request pattern)
        
        Returns:
            Session ID if matching session found, None otherwise
        """
        if not self.redis_client:
            return None

        try:
            # Normalize user agent for matching
            normalized_ua = self._normalize_user_agent_for_session(client_user_agent)
            
            # Search for connections with consolidated session data
            pattern = "vod_persistent_connection:*"
            cursor = 0
            matching_sessions = []

            while True:
                cursor, keys = self.redis_client.scan(cursor, match=pattern, count=100)

                for key in keys:
                    try:
                        connection_data = self.redis_client.hgetall(key)
                        if not connection_data:
                            continue

                        # Convert bytes keys/values to strings if needed
                        if isinstance(list(connection_data.keys())[0], bytes):
                            connection_data = {k.decode('utf-8'): v.decode('utf-8') for k, v in connection_data.items()}

                        # Check if content matches (using consolidated data)
                        stored_content_type = connection_data.get('content_obj_type', '')
                        stored_content_uuid = connection_data.get('content_uuid', '')

                        if stored_content_type != content_type or stored_content_uuid != content_uuid:
                            continue

                        # Extract session ID
                        session_id = key.decode('utf-8').replace('vod_persistent_connection:', '')

                        # Check if Redis-backed connection exists and has state
                        redis_connection = RedisBackedVODConnection(session_id, self.redis_client)
                        connection_state = redis_connection._get_connection_state()
                        
                        # Skip if connection state doesn't exist (was cleaned up)
                        if not connection_state:
                            logger.debug(f"[{session_id}] Connection state not found (likely cleaned up), skipping")
                            continue
                        
                        # Check if connection is stale (last activity > 10 seconds ago and no active streams)
                        # This helps avoid matching sessions that are in the process of being cleaned up
                        current_time = time.time()
                        time_since_activity = current_time - connection_state.last_activity
                        if time_since_activity > 10 and connection_state.active_streams == 0:
                            logger.debug(f"[{session_id}] Connection is stale (last activity {time_since_activity:.1f}s ago), skipping")
                            continue
                        
                        has_active = redis_connection.has_active_streams()
                        
                        # Skip if has active streams and we don't allow them
                        if has_active and not allow_active_streams:
                            continue

                        # Calculate match score
                        score = 10  # Content match
                        match_reasons = ["content"]

                        # Check other criteria (using consolidated data)
                        stored_client_ip = connection_data.get('client_ip', '')
                        stored_user_agent = connection_data.get('client_user_agent', '') or connection_data.get('user_agent', '')
                        stored_normalized_ua = self._normalize_user_agent_for_session(stored_user_agent)

                        # Exact IP match
                        if stored_client_ip and stored_client_ip == client_ip:
                            score += 5
                            match_reasons.append("ip")
                        
                        # Normalized user agent match (groups Jellyfin-related UAs)
                        if stored_normalized_ua == normalized_ua:
                            score += 3
                            match_reasons.append("user-agent")

                        # Check timeshift parameters (using consolidated data)
                        stored_utc_start = connection_data.get('utc_start', '')
                        stored_utc_end = connection_data.get('utc_end', '')
                        stored_offset = connection_data.get('offset', '')

                        current_utc_start = utc_start or ""
                        current_utc_end = utc_end or ""
                        current_offset = str(offset) if offset else ""

                        if (stored_utc_start == current_utc_start and
                            stored_utc_end == current_utc_end and
                            stored_offset == current_offset):
                            score += 7
                            match_reasons.append("timeshift")

                        # For exact client+content matches, allow even with active streams
                        # This handles Jellyfin's rapid request pattern
                        if score >= 18:  # content(10) + ip(5) + user-agent(3) = 18 (exact match)
                            matching_sessions.append({
                                'session_id': session_id,
                                'score': score,
                                'reasons': match_reasons,
                                'last_activity': float(connection_data.get('last_activity', '0')),
                                'has_active_streams': has_active
                            })
                        elif score >= 13 and not has_active:  # Good match but only if idle
                            matching_sessions.append({
                                'session_id': session_id,
                                'score': score,
                                'reasons': match_reasons,
                                'last_activity': float(connection_data.get('last_activity', '0')),
                                'has_active_streams': has_active
                            })

                    except Exception as e:
                        logger.debug(f"Error processing connection key {key}: {e}")
                        continue

                if cursor == 0:
                    break

            # Sort by: exact matches first (score >= 18), then by score, then by last activity
            # Prefer sessions without active streams when scores are equal
            matching_sessions.sort(key=lambda x: (
                x['score'] >= 18,  # Exact matches first
                x['score'],
                not x['has_active_streams'],  # Prefer idle sessions
                x['last_activity']
            ), reverse=True)

            if matching_sessions:
                best_match = matching_sessions[0]
                status = "active" if best_match['has_active_streams'] else "idle"
                logger.info(f"Found matching Redis-backed {status} session: {best_match['session_id']} "
                          f"(score: {best_match['score']}, reasons: {', '.join(best_match['reasons'])})")
                return best_match['session_id']

            return None

        except Exception as e:
            logger.error(f"Error finding matching idle session: {e}")
            return None

    def get_session_info(self, session_id: str) -> Optional[dict]:
        """Get session information from consolidated connection state (compatibility method)"""
        if not self.redis_client:
            return None

        try:
            redis_connection = RedisBackedVODConnection(session_id, self.redis_client)
            return redis_connection.get_session_metadata()
        except Exception as e:
            logger.error(f"Error getting session info for {session_id}: {e}")
            return None