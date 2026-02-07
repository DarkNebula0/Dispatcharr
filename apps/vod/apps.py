from django.apps import AppConfig


class VODConfig(AppConfig):
    default_auto_field = 'django.db.models.BigAutoField'
    name = 'apps.vod'
    verbose_name = 'Video on Demand'

    def ready(self):
        """Initialize VOD app when Django is ready"""
        # Import models to ensure they're registered
        from . import models
        # Import signals to register them
        from . import signals
        
        # Validate STRM configuration
        self._validate_strm_config()
        
        # Verify signal registration
        self._verify_signals()
    
    def _validate_strm_config(self):
        """Validate STRM file generation configuration"""
        import os
        import logging
        from django.conf import settings
        from pathlib import Path
        
        logger = logging.getLogger(__name__)
        
        # Validate STRM_BASE_URL
        strm_base_url = getattr(settings, 'STRM_BASE_URL', None)
        if not strm_base_url:
            is_debug = getattr(settings, 'DEBUG', False)
            if not is_debug:
                logger.error(
                    "STRM_BASE_URL is not configured. STRM file generation will fail. "
                    "Please set the STRM_BASE_URL environment variable."
                )
            else:
                logger.warning("STRM_BASE_URL not set, using default for development")
        else:
            # Validate URL format
            if not (strm_base_url.startswith('http://') or strm_base_url.startswith('https://')):
                logger.warning(f"STRM_BASE_URL '{strm_base_url}' does not appear to be a valid URL")
        
        # Validate STRM_MEDIA_ROOT is writable
        strm_media_root = Path(getattr(settings, 'STRM_MEDIA_ROOT', settings.MEDIA_ROOT))
        try:
            # Check if directory exists and is writable
            if strm_media_root.exists():
                if not os.access(strm_media_root, os.W_OK):
                    logger.error(
                        f"STRM_MEDIA_ROOT '{strm_media_root}' is not writable. "
                        "STRM file generation will fail."
                    )
                else:
                    logger.debug(f"STRM_MEDIA_ROOT '{strm_media_root}' is writable")
            else:
                # Try to create parent directory to check permissions
                try:
                    strm_media_root.parent.mkdir(parents=True, exist_ok=True)
                    logger.debug(f"STRM_MEDIA_ROOT parent directory is writable")
                except PermissionError:
                    logger.error(
                        f"Cannot create STRM_MEDIA_ROOT '{strm_media_root}'. "
                        "Check directory permissions."
                    )
        except Exception as e:
            logger.warning(f"Error validating STRM_MEDIA_ROOT: {e}")
    
    def _verify_signals(self):
        """Verify that signals are properly registered"""
        import logging
        from django.db.models.signals import post_save
        from .models import Movie, Episode
        
        logger = logging.getLogger(__name__)
        
        # Check if signal handlers are connected
        movie_handlers = post_save._live_receivers(Movie)
        episode_handlers = post_save._live_receivers(Episode)
        
        if not movie_handlers:
            logger.warning("No post_save signal handlers found for Movie model")
        else:
            logger.debug(f"Found {len(movie_handlers)} post_save signal handler(s) for Movie model")
        
        if not episode_handlers:
            logger.warning("No post_save signal handlers found for Episode model")
        else:
            logger.debug(f"Found {len(episode_handlers)} post_save signal handler(s) for Episode model")