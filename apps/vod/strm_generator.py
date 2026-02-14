"""
STRM file generator for VOD content.

Generates STRM files (text files containing streaming URLs) and Kodi-compatible
NFO files for movies and TV episodes, organized in a media server-friendly structure.
"""
import os
import re
import logging
import unicodedata
import uuid
import xml.etree.ElementTree as ET
import xml.sax.saxutils
from pathlib import Path
from django.conf import settings
from django.contrib.contenttypes.models import ContentType
from django.db import transaction
from .models import STRMFile, Movie, Episode, Series

# File locking support
HAS_FCNTL = False
HAS_MSVCRT = False

try:
    import fcntl
    HAS_FCNTL = True
except ImportError:
    try:
        import msvcrt
        HAS_MSVCRT = True
    except ImportError:
        pass

logger = logging.getLogger(__name__)

try:
    from guessit import guessit
except ImportError:
    guessit = None
    logger.warning("guessit library not available. Name sanitization will be limited.")

# Resolution detection patterns (authoritative, not heuristic)
RESOLUTION_ALIASES = {
    r"(2160p|3840p|uhd|ultra\s?hd|4k|⁴ᴷ|³⁸⁴⁰ᴾ)": "4k",
    r"(1080p|full\s?hd|fhd)": "1080p",
    r"(720p|hd)": "720p",
}

# Language prefix pattern
LANG_PREFIX = r"^(DE|EN|FR|ES|JA)\s*-\s*(.+)"


def get_movie_category(movie):
    """
    Get category from the first M3U relation for a movie.
    
    Args:
        movie: Movie model instance
        
    Returns:
        VODCategory instance or None if no category found
    """
    try:
        first_relation = movie.m3u_relations.select_related('category').first()
        if first_relation and first_relation.category:
            return first_relation.category
    except Exception as e:
        logger.debug(f"Error getting category for movie {movie.name}: {e}")
    return None


def get_series_category(series):
    """
    Get category from the first M3U relation for a series.
    
    Args:
        series: Series model instance
        
    Returns:
        VODCategory instance or None if no category found
    """
    try:
        first_relation = series.m3u_relations.select_related('category').first()
        if first_relation and first_relation.category:
            return first_relation.category
    except Exception as e:
        logger.debug(f"Error getting category for series {series.name}: {e}")
    return None


def detect_resolution(text):
    """
    Detect resolution from text using regex patterns with unicode normalization.
    
    Checks for resolution indicators like 4k, 1080p, 720p, etc., including
    unicode variants (⁴ᴷ, ³⁸⁴⁰ᴾ, etc.).
    
    Args:
        text: Text to search for resolution indicators (can be None)
        
    Returns:
        Resolution string ("4k", "1080p", "720p") or None
    """
    if not text:
        return None
    
    # Normalize unicode (NFKD decomposition)
    text = unicodedata.normalize("NFKD", str(text)).lower()
    
    # Check patterns in order of priority (4k first, then 1080p, then 720p)
    for pattern, label in RESOLUTION_ALIASES.items():
        if re.search(pattern, text, re.IGNORECASE):
            return label
    
    return None


def clean_filename(name):
    """
    Clean filename by removing tmdb tags, duplicate years, and normalizing spaces.
    
    Args:
        name: Filename to clean
        
    Returns:
        Cleaned filename
    """
    if not name:
        return ""
    
    # Remove tmdb tags
    name = re.sub(r"\{tmdb-\d+\}", "", name, flags=re.IGNORECASE)
    
    # Remove duplicate years: (1988) (1988) -> (1988)
    name = re.sub(r"\((\d{4})\)\s*\(\1\)", r"(\1)", name)
    
    # Normalize spaces
    name = re.sub(r"\s+", " ", name)
    
    return name.strip()


def extract_language_prefix(name):
    """
    Extract language prefix from filename (DE, EN, FR, ES, JA).
    
    Handles cases where quality/resolution prefixes come before language code
    (e.g., "4K-DE - Title" or "1080p-EN - Title").
    
    Args:
        name: Filename to extract language from
        
    Returns:
        Tuple of (name_without_prefix, language_code) or (name, None)
    """
    if not name:
        return name, None
    
    # First try direct match at start
    match = re.match(LANG_PREFIX, name, re.IGNORECASE)
    if match:
        return match.group(2), match.group(1).lower()
    
    # Try to find language prefix after quality/resolution prefix
    # Pattern: quality/resolution prefix (optional) + language code
    # Examples: "4K-DE - Title", "1080p-EN - Title", "4K DE - Title"
    quality_lang_pattern = r'^[^-]*-?(?:4k|1080p|720p|2160p|uhd|fhd|hd)\s*-?\s*(DE|EN|FR|ES|JA)\s*-\s*(.+)'
    match = re.match(quality_lang_pattern, name, re.IGNORECASE)
    if match:
        return match.group(2), match.group(1).lower()
    
    # Try pattern with space: "4K DE - Title"
    quality_lang_pattern2 = r'^(?:4k|1080p|720p|2160p|uhd|fhd|hd)\s+(DE|EN|FR|ES|JA)\s*-\s*(.+)'
    match = re.match(quality_lang_pattern2, name, re.IGNORECASE)
    if match:
        return match.group(2), match.group(1).lower()
    
    # Try pattern with dash: "4K-DE - Title"
    quality_lang_pattern3 = r'^(?:4k|1080p|720p|2160p|uhd|fhd|hd)-(DE|EN|FR|ES|JA)\s*-\s*(.+)'
    match = re.match(quality_lang_pattern3, name, re.IGNORECASE)
    if match:
        return match.group(2), match.group(1).lower()
    
    return name, None


def detect_quality_with_context(movie, parent_context=None, relation=None):
    """
    Detect quality/resolution from custom_properties, parent context, or movie name.
    
    Uses parent folder context + filename for better detection (like reference code).
    Checks custom_properties first, then combines parent context + filename.
    
    Args:
        movie: Movie model instance
        parent_context: Optional parent folder name for context
        relation: Optional M3UMovieRelation instance
        
    Returns:
        Resolution string ("4k", "1080p", "720p") or None
    """
    # First, try to get quality from custom_properties
    if relation and relation.custom_properties:
        custom_props = relation.custom_properties
        quality = custom_props.get('quality') or custom_props.get('resolution')
        if quality:
            resolution = detect_resolution(quality)
            if resolution:
                return resolution
    
    # If no relation provided, try first relation
    if not relation:
        relation = movie.m3u_relations.first()
        if relation and relation.custom_properties:
            custom_props = relation.custom_properties
            quality = custom_props.get('quality') or custom_props.get('resolution')
            if quality:
                resolution = detect_resolution(quality)
                if resolution:
                    return resolution
    
    # Combine parent context + filename for detection (VERY IMPORTANT per reference)
    combined = ""
    if parent_context:
        combined = f"{parent_context} "
    if movie.name:
        combined += movie.name
    else:
        return None
    
    # Normalize and detect resolution
    resolution = detect_resolution(combined)
    if resolution:
        return resolution
    
    # Fallback: parse movie name with guessit
    if guessit and movie.name:
        try:
            parsed = guessit(movie.name)
            if parsed.get('screen_size'):
                screen_size = str(parsed['screen_size'])
                return detect_resolution(screen_size)
        except Exception as e:
            logger.debug(f"Error parsing quality from movie name {movie.name}: {e}")
    
    # Final fallback: check name directly
    return detect_resolution(movie.name)


def detect_series_quality_with_context(series, parent_context=None, relation=None):
    """
    Detect quality/resolution from custom_properties, parent context, or series name.
    
    Similar to detect_quality_with_context but for series.
    
    Args:
        series: Series model instance
        parent_context: Optional parent folder name for context
        relation: Optional M3USeriesRelation instance
        
    Returns:
        Resolution string ("4k", "1080p", "720p") or None
    """
    # First, try to get quality from custom_properties
    if relation and relation.custom_properties:
        custom_props = relation.custom_properties
        quality = custom_props.get('quality') or custom_props.get('resolution')
        if quality:
            resolution = detect_resolution(quality)
            if resolution:
                return resolution
    
    # If no relation provided, try first relation
    if not relation:
        relation = series.m3u_relations.first()
        if relation and relation.custom_properties:
            custom_props = relation.custom_properties
            quality = custom_props.get('quality') or custom_props.get('resolution')
            if quality:
                resolution = detect_resolution(quality)
                if resolution:
                    return resolution
    
    # Combine parent context + filename for detection
    combined = ""
    if parent_context:
        combined = f"{parent_context} "
    if series.name:
        combined += series.name
    else:
        return None
    
    # Normalize and detect resolution
    resolution = detect_resolution(combined)
    if resolution:
        return resolution
    
    # Fallback: parse series name with guessit
    if guessit and series.name:
        try:
            parsed = guessit(series.name)
            if parsed.get('screen_size'):
                screen_size = str(parsed['screen_size'])
                return detect_resolution(screen_size)
        except Exception as e:
            logger.debug(f"Error parsing quality from series name {series.name}: {e}")
    
    # Final fallback: check name directly
    return detect_resolution(series.name)


def is_anime(movie_or_series, info=None, category=None):
    """
    Determine if content is anime.
    
    Checks in order:
    1. Database category (if category name contains animation-related terms)
    2. Genre field (if genre contains "Animation" or "Anime")
    3. Language detection (ja = Japanese)
    4. Known anime studios in title/description
    5. Known anime titles list (optional)
    
    Args:
        movie_or_series: Movie or Series model instance
        info: Optional parsed info dict from guessit
        category: Optional VODCategory instance
        
    Returns:
        True if anime, False otherwise
    """
    # Animation-related category keywords (case-insensitive matching)
    animation_keywords = [
        'anime',
        'animation',
        'cartoon',
        'ghibli',
        'studio ghibli',
        'anime movie',
        'animated',
    ]
    
    # Known anime studios (for checking in title/description)
    anime_studios = [
        'studio ghibli',
        'ghibli',
        'madhouse',
        'bones',
        'ufotable',
        'kyoto animation',
        'a-1 pictures',
        'production i.g',
        'wit studio',
        'trigger',
        'gainax',
    ]
    
    # Check database category first (expanded matching)
    if category:
        cat_name = category.name.lower()
        for keyword in animation_keywords:
            if keyword in cat_name:
                return True
    
    # If no category provided, try to get from relations
    if not category:
        if hasattr(movie_or_series, 'm3u_relations'):
            first_relation = movie_or_series.m3u_relations.select_related('category').first()
            if first_relation and first_relation.category:
                cat_name = first_relation.category.name.lower()
                for keyword in animation_keywords:
                    if keyword in cat_name:
                        return True
    
    # Check genre field (can be comma-separated)
    if hasattr(movie_or_series, 'genre') and movie_or_series.genre:
        genre_str = str(movie_or_series.genre).lower()
        # Split on comma and check each genre
        genres = [g.strip() for g in genre_str.split(',')]
        for genre in genres:
            if 'animation' in genre or 'anime' in genre:
                return True
    
    # Check language from info (ja = Japanese)
    if info and info.get('language'):
        lang = str(info['language']).lower()
        if lang == 'ja' or lang == 'japanese':
            return True
    
    # Check title/description for known anime studios
    title = ""
    description = ""
    
    if info and info.get('title'):
        title = str(info['title']).lower()
    elif hasattr(movie_or_series, 'name'):
        title = movie_or_series.name.lower()
    
    if hasattr(movie_or_series, 'description') and movie_or_series.description:
        description = str(movie_or_series.description).lower()
    
    # Check if any anime studio appears in title or description
    combined_text = f"{title} {description}".lower()
    for studio in anime_studios:
        if studio in combined_text:
            return True
    
    # Check title for known anime titles (optional, can be extended)
    known_anime_titles = {
        "akira",
        "belle",
        "ghost in the shell",
        "your name",
        "weathering with you",
        "spirited away",
        "princess mononoke",
        "howl's moving castle",
    }
    
    if title in known_anime_titles:
        return True
    
    return False


def decide_folder(content_type, is_anime, is_4k):
    """
    Decide which folder to use based on content type, anime status, and 4K status.
    
    Returns simple folder names:
    - anime4k: is_anime=True AND is_4k=True
    - anime: is_anime=True AND is_4k=False
    - 4k: is_anime=False AND is_4k=True
    - standard: Everything else
    
    Args:
        content_type: "movie" or "episode"
        is_anime: Boolean indicating if content is anime
        is_4k: Boolean indicating if content is 4K (resolution == "4k")
        
    Returns:
        Folder name string
    """
    if is_anime and is_4k:
        return "anime4k"
    if is_anime:
        return "anime"
    if is_4k:
        return "4k"
    return "standard"


def parse_media_with_context(name, parent_context=None):
    """
    Parse media name with parent folder context using guessit.
    
    Uses parent folder context ONLY for resolution detection, not for title parsing.
    Extracts language prefix, cleans filename, and uses guessit for parsing.
    
    Args:
        name: Media name/filename
        parent_context: Optional parent folder name for context (used only for resolution detection)
        
    Returns:
        Dictionary with parsed info (title, year, language, screen_size, etc.)
    """
    if not name:
        return {}
    
    # Use parent context ONLY for resolution detection (combine for that purpose)
    combined_for_resolution = ""
    if parent_context:
        combined_for_resolution = f"{parent_context} "
    combined_for_resolution += name
    
    # Normalize unicode for resolution detection
    combined_normalized = unicodedata.normalize("NFKD", combined_for_resolution)
    
    # Hard resolution detection from combined text
    resolution = detect_resolution(combined_normalized)
    
    # For title parsing, use ONLY the filename (not parent context)
    # Clean only the filename part for guessit
    raw = clean_filename(name)
    raw, lang = extract_language_prefix(raw)
    
    # Store cleaned raw name as fallback for title
    cleaned_name = raw.strip()
    
    # Parse with guessit
    info = {}
    if guessit:
        try:
            info = guessit(
                raw,
                options={
                    "type": "movie",  # Will be overridden for episodes if needed
                    "expected_language": ["de", "en", "ja", "fr", "es"],
                }
            )
        except Exception as e:
            logger.debug(f"Error parsing with guessit: {e}")
    
    # If guessit didn't return a title, extract it manually from cleaned name
    if not info.get('title'):
        # Extract year first if present
        year_match = re.search(r'\((\d{4})\)', cleaned_name)
        if year_match:
            info['year'] = int(year_match.group(1))
        
        # Extract title by removing year and quality/resolution tags
        title_candidate = cleaned_name
        
        # Remove quality/resolution prefixes at start (e.g., "4K-", "1080p-", "4K ")
        # This handles cases where quality prefix wasn't removed by language extraction
        title_candidate = re.sub(r'^(?:4k|1080p|720p|2160p|uhd|fhd|hd)[\s-]+', '', title_candidate, flags=re.IGNORECASE)
        
        # Remove year: (1988) or (1988) (4K) etc.
        title_candidate = re.sub(r'\s*\((\d{4})\)', '', title_candidate)
        
        # Remove quality/resolution tags in parentheses
        title_candidate = re.sub(r'\s*\(4k\)', '', title_candidate, flags=re.IGNORECASE)
        title_candidate = re.sub(r'\s*\(1080p\)', '', title_candidate, flags=re.IGNORECASE)
        title_candidate = re.sub(r'\s*\(720p\)', '', title_candidate, flags=re.IGNORECASE)
        title_candidate = re.sub(r'\s*\(2160p\)', '', title_candidate, flags=re.IGNORECASE)
        title_candidate = re.sub(r'\s*\(uhd\)', '', title_candidate, flags=re.IGNORECASE)
        
        # Clean up extra spaces and trim
        title_candidate = re.sub(r'\s+', ' ', title_candidate).strip()
        
        if title_candidate:
            info['title'] = title_candidate
        else:
            # Last resort: use cleaned name as-is
            info['title'] = cleaned_name
    
    # Override/add language if extracted from prefix
    if lang:
        info["language"] = lang
    
    # Override/add resolution if detected
    if resolution:
        info["screen_size"] = resolution
    
    return info


def build_jellyfin_name(info, tmdb_id=None):
    """
    Build Jellyfin-friendly name with brackets for metadata.
    
    Format: Title (Year) [LANG] [RESOLUTION] [tmdb-123]
    
    Args:
        info: Dictionary with parsed info (title, year, language, screen_size)
        tmdb_id: Optional TMDB ID to include
        
    Returns:
        Formatted name string
    """
    title = info.get('title')
    if not title or title == 'Unknown Title':
        # Fallback: try to construct from available info
        title = "Unknown Title"
    
    year = info.get('year')
    lang = info.get('language')
    res = info.get('screen_size')
    
    parts = [title]
    
    if year:
        parts.append(f"({year})")
    if lang:
        parts.append(f"[{lang.upper()}]")
    if res:
        parts.append(f"[{res.upper()}]")
    if tmdb_id:
        parts.append(f"[tmdb-{tmdb_id}]")
    
    return " ".join(parts)


def sanitize_movie_name_with_guessit(movie):
    """
    Parse and sanitize movie name using guessit, removing language codes and quality tags.
    
    Extracts clean title, year, and keeps TMDB ID if available.
    Example: "DE - Akira (1988) (4K)" → "Akira (1988)"
    
    Args:
        movie: Movie model instance
        
    Returns:
        Dictionary with 'title', 'year', and 'folder_name' keys
    """
    if not guessit:
        # Fallback to existing logic if guessit not available
        title = movie.name
        year = movie.year
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        if movie.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{movie.tmdb_id}}}"
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }
    
    try:
        parsed = guessit(movie.name)
        
        # Extract clean title (without language codes, quality tags, etc.)
        title = parsed.get('title') or movie.name
        
        # Remove common prefixes/suffixes that might interfere with matching
        # Remove language codes if they're at the start
        title = re.sub(r'^[A-Z]{2}\s*-\s*', '', title, flags=re.IGNORECASE)
        
        # Use year from guessit if available, otherwise use movie.year
        year = parsed.get('year') or movie.year
        
        # Build folder name
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        
        # Add TMDB ID if available (helps media servers match content)
        if movie.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{movie.tmdb_id}}}"
        
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }
    except Exception as e:
        logger.warning(f"Error parsing movie name with guessit for {movie.name}: {e}, using fallback")
        # Fallback to existing logic
        title = movie.name
        year = movie.year
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        if movie.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{movie.tmdb_id}}}"
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }


def sanitize_series_name_with_guessit(series):
    """
    Parse and sanitize series name using guessit.
    
    Args:
        series: Series model instance
        
    Returns:
        Dictionary with 'title', 'year', and 'folder_name' keys
    """
    if not guessit:
        # Fallback to existing logic
        title = series.name
        year = series.year
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        if series.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{series.tmdb_id}}}"
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }
    
    try:
        parsed = guessit(series.name)
        
        # Extract clean title
        title = parsed.get('title') or series.name
        
        # Remove language codes if they're at the start
        title = re.sub(r'^[A-Z]{2}\s*-\s*', '', title, flags=re.IGNORECASE)
        
        # Use year from guessit if available, otherwise use series.year
        year = parsed.get('year') or series.year
        
        # Build folder name
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        
        # Add TMDB ID if available
        if series.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{series.tmdb_id}}}"
        
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }
    except Exception as e:
        logger.warning(f"Error parsing series name with guessit for {series.name}: {e}, using fallback")
        # Fallback to existing logic
        title = series.name
        year = series.year
        folder_name = title
        if year:
            folder_name = f"{folder_name} ({year})"
        if series.tmdb_id:
            folder_name = f"{folder_name} {{tmdb-{series.tmdb_id}}}"
        return {
            'title': title,
            'year': year,
            'folder_name': sanitize_filename(folder_name)
        }


def sanitize_filename(name):
    """
    Sanitize a filename to be filesystem-safe.
    
    Removes or replaces invalid characters for filesystem compatibility.
    
    Args:
        name: The filename to sanitize
        
    Returns:
        A sanitized filename safe for use in filesystems
    """
    # Remove or replace invalid characters
    # Windows: < > : " / \ | ? *
    # Unix: / and null
    invalid_chars = r'[<>:"/\\|?*\x00]'
    sanitized = re.sub(invalid_chars, '', name)
    
    # Remove leading/trailing spaces and dots (Windows doesn't allow these)
    sanitized = sanitized.strip(' .')
    
    # Replace multiple spaces with single space
    sanitized = re.sub(r'\s+', ' ', sanitized)
    
    # Limit length (Windows has 255 char limit for filename)
    if len(sanitized) > 200:
        sanitized = sanitized[:200]
    
    return sanitized


def get_movie_folder_name(movie):
    """
    Generate folder name for a movie using guessit sanitization.
    
    Format: "Movie Name (Year) {tmdb-12345}" (if tmdb_id available)
    Format: "Movie Name (Year)" (if no tmdb_id)
    
    Uses guessit to clean the name, removing language codes and quality tags.
    
    Args:
        movie: Movie model instance
        
    Returns:
        Sanitized folder name
    """
    sanitized = sanitize_movie_name_with_guessit(movie)
    return sanitized['folder_name']


def get_episode_filename(episode):
    """
    Generate filename for an episode.
    
    Format: "Series Name (Year) - S01E01 - Episode Name"
    
    Args:
        episode: Episode model instance
        
    Returns:
        Sanitized filename (without extension)
    """
    series = episode.series
    series_name = series.name
    if series.year:
        series_name = f"{series_name} ({series.year})"
    
    season_num = episode.season_number or 0
    episode_num = episode.episode_number or 0
    season_ep = f"S{season_num:02d}E{episode_num:02d}"
    
    episode_name = episode.name or ""
    
    if episode_name:
        filename = f"{series_name} - {season_ep} - {episode_name}"
    else:
        filename = f"{series_name} - {season_ep}"
    
    return sanitize_filename(filename)


def get_base_url():
    """
    Get the base URL for STRM file URLs.
    
    Returns:
        Base URL string from settings
    
    Raises:
        ValueError: If STRM_BASE_URL is not set in production
    """
    base_url = getattr(settings, 'STRM_BASE_URL', None)
    if not base_url:
        is_debug = getattr(settings, 'DEBUG', False)
        if is_debug:
            base_url = 'http://localhost:5656'
            logger.warning("STRM_BASE_URL not set, using default localhost URL for development")
        else:
            raise ValueError(
                "STRM_BASE_URL is required in production. "
                "Please set the STRM_BASE_URL environment variable to your server's base URL."
            )
    return base_url


def generate_strm_file(file_path, url):
    """
    Generate a STRM file with the streaming URL.
    
    Uses file-level locking to prevent race conditions when multiple processes
    try to write the same file simultaneously.
    
    Args:
        file_path: Path where the STRM file should be created
        url: The streaming URL to write to the file
    """
    try:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        # Use exclusive file locking to prevent race conditions
        with open(file_path, 'w', encoding='utf-8') as f:
            # Acquire exclusive lock (non-blocking)
            if HAS_FCNTL:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif HAS_MSVCRT:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            
            try:
                f.write(url)
                f.flush()
            finally:
                # Release lock
                if HAS_FCNTL:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                elif HAS_MSVCRT:
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        
        logger.debug(f"Created STRM file: {file_path}")
    except (IOError, OSError) as e:
        # Lock acquisition failed - another process is writing
        if "Resource temporarily unavailable" in str(e) or "would block" in str(e).lower():
            logger.debug(f"STRM file {file_path} is being written by another process, skipping")
            return
        logger.error(f"Failed to create STRM file {file_path}: {e}", exc_info=True)
        raise
    except Exception as e:
        logger.error(f"Failed to create STRM file {file_path}: {e}", exc_info=True)
        raise


def generate_nfo_file(file_path, content_type, metadata):
    """
    Generate a Kodi-compatible NFO file.
    
    Args:
        file_path: Path where the NFO file should be created
        content_type: Either 'movie' or 'episode'
        metadata: Dictionary containing metadata (title, plot, year, etc.)
    """
    try:
        file_path = Path(file_path)
        file_path.parent.mkdir(parents=True, exist_ok=True)
        
        if content_type == 'movie':
            root = ET.Element('movie')
        elif content_type == 'episode':
            root = ET.Element('episodedetails')
        else:
            raise ValueError(f"Invalid content_type: {content_type}")
        
        # Helper function to safely set text content with XML escaping
        def set_text(element, value):
            """Set text content with proper XML escaping"""
            if value is not None:
                # Convert to string and escape XML special characters
                text = str(value)
                element.text = xml.sax.saxutils.escape(text)
        
        # Title
        if metadata.get('title'):
            set_text(ET.SubElement(root, 'title'), metadata['title'])
        
        # Plot/Description
        if metadata.get('plot'):
            set_text(ET.SubElement(root, 'plot'), metadata['plot'])
        elif metadata.get('description'):
            set_text(ET.SubElement(root, 'plot'), metadata['description'])
        
        # Year
        if metadata.get('year'):
            set_text(ET.SubElement(root, 'year'), metadata['year'])
        
        # Rating
        if metadata.get('rating'):
            rating_elem = ET.SubElement(root, 'rating')
            set_text(ET.SubElement(rating_elem, 'value'), metadata['rating'])
            ET.SubElement(rating_elem, 'votes').text = '0'
        
        # Genre
        if metadata.get('genre'):
            genre_str = str(metadata['genre'])
            # Split comma-separated genres
            for genre in genre_str.split(','):
                genre = genre.strip()
                if genre:
                    set_text(ET.SubElement(root, 'genre'), genre)
        
        # TMDB ID
        if metadata.get('tmdb_id'):
            set_text(ET.SubElement(root, 'tmdbid'), metadata['tmdb_id'])
        
        # IMDB ID
        if metadata.get('imdb_id'):
            set_text(ET.SubElement(root, 'imdbid'), metadata['imdb_id'])
        
        # Thumbnail/Poster
        if metadata.get('thumbnail'):
            set_text(ET.SubElement(root, 'thumb'), metadata['thumbnail'])
        elif metadata.get('poster'):
            set_text(ET.SubElement(root, 'thumb'), metadata['poster'])
        
        # Episode-specific fields
        if content_type == 'episode':
            if metadata.get('season'):
                set_text(ET.SubElement(root, 'season'), metadata['season'])
            if metadata.get('episode'):
                set_text(ET.SubElement(root, 'episode'), metadata['episode'])
            if metadata.get('aired'):
                set_text(ET.SubElement(root, 'aired'), metadata['aired'])
            if metadata.get('showtitle'):
                set_text(ET.SubElement(root, 'showtitle'), metadata['showtitle'])
        
        # Write XML to file with file-level locking
        tree = ET.ElementTree(root)
        ET.indent(tree, space='  ')
        
        # Use exclusive file locking to prevent race conditions
        with open(file_path, 'wb') as f:
            # Acquire exclusive lock (non-blocking)
            if HAS_FCNTL:
                fcntl.flock(f.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            elif HAS_MSVCRT:
                msvcrt.locking(f.fileno(), msvcrt.LK_NBLCK, 1)
            
            try:
                tree.write(f, encoding='utf-8', xml_declaration=True)
                f.flush()
            finally:
                # Release lock
                if HAS_FCNTL:
                    fcntl.flock(f.fileno(), fcntl.LOCK_UN)
                elif HAS_MSVCRT:
                    msvcrt.locking(f.fileno(), msvcrt.LK_UNLCK, 1)
        
        logger.debug(f"Created NFO file: {file_path}")
    except Exception as e:
        logger.error(f"Failed to create NFO file {file_path}: {e}", exc_info=True)
        raise


def generate_movie_strm(movie):
    """
    Generate STRM and NFO files for a movie.
    
    Creates:
    - media/movies/{folder}/Movie Name (Year) [LANG] [RES] [tmdb-123]/Movie Name (Year) [LANG] [RES] [tmdb-123].strm
    - media/movies/{folder}/Movie Name (Year) [LANG] [RES] [tmdb-123]/Movie Name (Year) [LANG] [RES] [tmdb-123].nfo
    
    Where folder is one of: anime4k, anime, 4k, standard
    
    Args:
        movie: Movie model instance
    """
    try:
        # Use get_or_create to handle race conditions atomically
        content_type = ContentType.objects.get_for_model(Movie)
        strm_file_record, created = STRMFile.objects.get_or_create(
            content_type=content_type,
            content_id=movie.id,
            defaults={'strm_path': '', 'nfo_path': None}
        )
        
        # Check if file exists on disk
        if not created and strm_file_record.strm_path and os.path.exists(strm_file_record.strm_path):
            # File exists, skip generation
            logger.debug(f"STRM file already exists for movie {movie.name} (ID: {movie.id}), skipping generation")
            return
        elif not created:
            # Record exists but file is missing, we'll regenerate
            logger.info(f"STRM file record exists but file missing for movie {movie.name}, regenerating")
        
        media_root = Path(getattr(settings, 'STRM_MEDIA_ROOT', settings.MEDIA_ROOT))
        base_url = get_base_url()
        
        # Get category and relation
        category = get_movie_category(movie)
        relation = movie.m3u_relations.first()
        
        # Parse media with context (parent folder would be category name if available)
        parent_context = category.name if category else None
        info = parse_media_with_context(movie.name, parent_context)
        
        # Detect resolution (with parent context)
        resolution = detect_quality_with_context(movie, parent_context, relation)
        if resolution:
            info['screen_size'] = resolution
        
        # Determine if anime
        is_anime_flag = is_anime(movie, info, category)
        
        # Determine if 4K
        is_4k = info.get('screen_size') == '4k'
        
        # Decide folder
        folder_name = decide_folder('movie', is_anime_flag, is_4k)
        
        # Build Jellyfin-friendly name
        final_name = build_jellyfin_name(info, movie.tmdb_id)
        final_name = sanitize_filename(final_name)
        
        # Build full path: media/movies/{folder}/{final_name}/
        # Handle path collisions by appending UUID if directory already exists
        base_movies_dir = media_root / 'movies' / folder_name / final_name
        movies_dir = base_movies_dir
        
        # Check for path collision - if directory exists and has different content, append UUID
        if movies_dir.exists() and movies_dir.is_dir():
            # Check if this directory belongs to a different movie
            existing_strm_files = list(movies_dir.glob('*.strm'))
            if existing_strm_files:
                # Check if the existing STRM file is for a different movie
                try:
                    existing_strm_path = str(existing_strm_files[0])
                    existing_record = STRMFile.objects.filter(strm_path=existing_strm_path).first()
                    if existing_record and existing_record.content_id != movie.id:
                        # Collision detected - append UUID to make path unique
                        unique_suffix = str(uuid.uuid4())[:8]
                        final_name = f"{final_name}_{unique_suffix}"
                        movies_dir = media_root / 'movies' / folder_name / final_name
                        logger.warning(f"Path collision detected for movie {movie.name}, using unique path: {final_name}")
                except Exception as e:
                    logger.debug(f"Error checking for path collision: {e}, proceeding with original path")
        
        movies_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate STRM file
        strm_filename = f"{final_name}.strm"
        strm_path = movies_dir / strm_filename
        
        # Build streaming URL
        base = base_url.rstrip('/')
        stream_url = f"{base}/proxy/vod/movie/{movie.uuid}"
        
        generate_strm_file(strm_path, stream_url)
        
        # Generate NFO file
        nfo_filename = f"{final_name}.nfo"
        nfo_path = movies_dir / nfo_filename
        
        # Build metadata with clean title (for metadata matching, not the Jellyfin name)
        # Use the parsed title from info - this should already be cleaned
        clean_title = info.get('title')
        if not clean_title or clean_title == 'Unknown Title':
            # Fallback: clean the movie name manually
            raw = clean_filename(movie.name)
            raw, _ = extract_language_prefix(raw)
            
            # Remove year and quality tags for clean title
            clean_title = raw
            clean_title = re.sub(r'\s*\((\d{4})\)', '', clean_title)  # Remove year
            clean_title = re.sub(r'\s*\(4k\)', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s*\(1080p\)', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s*\(720p\)', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s*\(2160p\)', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s*\(uhd\)', '', clean_title, flags=re.IGNORECASE)
            clean_title = re.sub(r'\s+', ' ', clean_title).strip()
            
            if not clean_title:
                clean_title = movie.name
        
        metadata = {
            'title': clean_title,  # Clean title for metadata matching
            'plot': movie.description,
            'year': info.get('year') or movie.year,
            'rating': movie.rating,
            'genre': movie.genre,
            'tmdb_id': movie.tmdb_id,
            'imdb_id': movie.imdb_id,
        }
        
        # Add thumbnail if available
        if movie.logo and movie.logo.url:
            metadata['thumbnail'] = movie.logo.url
        
        generate_nfo_file(nfo_path, 'movie', metadata)
        
        # Update STRMFile record with actual paths used
        # Use update_or_create to handle race condition where record might be deleted
        # by cleanup task between get_or_create and save
        strm_path_str = str(strm_path)
        nfo_path_str = str(nfo_path)
        
        STRMFile.objects.update_or_create(
            content_type=content_type,
            content_id=movie.id,
            defaults={
                'strm_path': strm_path_str,
                'nfo_path': nfo_path_str,
            }
        )
        
        logger.info(f"Generated STRM/NFO files for movie: {movie.name} (path: movies/{folder_name}/{final_name})")
    except Exception as e:
        logger.error(f"Failed to generate STRM files for movie {movie.name} (ID: {movie.id}): {e}", exc_info=True)
        # Don't raise - we don't want STRM generation failures to break the save operation


def generate_episode_strm(episode):
    """
    Generate STRM and NFO files for an episode.
    
    Creates:
    - media/tv/{folder}/Series Name (Year) [LANG] [RES] [tmdb-123]/Season XX/Episode Name.strm
    - media/tv/{folder}/Series Name (Year) [LANG] [RES] [tmdb-123]/Season XX/Episode Name.nfo
    
    Where folder is one of: anime4k, anime, 4k, standard
    
    Args:
        episode: Episode model instance
    """
    try:
        # Use get_or_create to handle race conditions atomically
        content_type = ContentType.objects.get_for_model(Episode)
        strm_file_record, created = STRMFile.objects.get_or_create(
            content_type=content_type,
            content_id=episode.id,
            defaults={'strm_path': '', 'nfo_path': None}
        )
        
        # Check if file exists on disk
        if not created and strm_file_record.strm_path and os.path.exists(strm_file_record.strm_path):
            # File exists, skip generation
            logger.debug(f"STRM file already exists for episode {episode} (ID: {episode.id}), skipping generation")
            return
        elif not created:
            # Record exists but file is missing, we'll regenerate
            logger.info(f"STRM file record exists but file missing for episode {episode}, regenerating")
        
        media_root = Path(getattr(settings, 'STRM_MEDIA_ROOT', settings.MEDIA_ROOT))
        base_url = get_base_url()
        
        series = episode.series
        
        # Get category and relation
        category = get_series_category(series)
        relation = series.m3u_relations.first()
        
        # Parse series with context (parent folder would be category name if available)
        parent_context = category.name if category else None
        info = parse_media_with_context(series.name, parent_context)
        
        # Override type for episodes
        if guessit:
            try:
                # Re-parse as episode type for better parsing
                raw = clean_filename(series.name)
                raw, lang = extract_language_prefix(raw)
                episode_info = guessit(
                    raw,
                    options={
                        "type": "episode",
                        "expected_language": ["de", "en", "ja", "fr", "es"],
                    }
                )
                if lang:
                    episode_info["language"] = lang
                # Merge with existing info
                info.update(episode_info)
            except Exception as e:
                logger.debug(f"Error re-parsing series as episode: {e}")
        
        # Detect resolution (with parent context)
        resolution = detect_series_quality_with_context(series, parent_context, relation)
        if resolution:
            info['screen_size'] = resolution
        
        # Determine if anime
        is_anime_flag = is_anime(series, info, category)
        
        # Determine if 4K
        is_4k = info.get('screen_size') == '4k'
        
        # Decide folder
        folder_name = decide_folder('episode', is_anime_flag, is_4k)
        
        # Build Jellyfin-friendly series name
        series_final_name = build_jellyfin_name(info, series.tmdb_id)
        series_final_name = sanitize_filename(series_final_name)
        
        # Generate season folder name
        season_num = episode.season_number or 0
        season_folder_name = f"Season {season_num:02d}"
        
        # Generate episode filename
        episode_name = episode.name or ""
        season_ep = f"S{season_num:02d}E{episode.episode_number or 0:02d}"
        if episode_name:
            episode_filename = f"{series_final_name} - {season_ep} - {episode_name}"
        else:
            episode_filename = f"{series_final_name} - {season_ep}"
        episode_filename = sanitize_filename(episode_filename)
        
        # Build paths: media/tv/{folder}/{series_name}/Season XX/
        # Handle path collisions by appending UUID if series directory already exists with different content
        base_tv_dir = media_root / 'tv' / folder_name / series_final_name
        tv_dir = base_tv_dir / season_folder_name
        
        # Check for path collision at series level - if directory exists and has different content, append UUID
        if base_tv_dir.exists() and base_tv_dir.is_dir():
            # Check if this series directory belongs to a different series
            existing_strm_files = list(base_tv_dir.rglob('*.strm'))
            if existing_strm_files:
                # Check if the existing STRM file is for a different series
                try:
                    existing_strm_path = str(existing_strm_files[0])
                    existing_record = STRMFile.objects.filter(strm_path=existing_strm_path).first()
                    if existing_record:
                        existing_episode = existing_record.content_object
                        if existing_episode and hasattr(existing_episode, 'series'):
                            if existing_episode.series.id != series.id:
                                # Collision detected - append UUID to make path unique
                                unique_suffix = str(uuid.uuid4())[:8]
                                series_final_name = f"{series_final_name}_{unique_suffix}"
                                base_tv_dir = media_root / 'tv' / folder_name / series_final_name
                                tv_dir = base_tv_dir / season_folder_name
                                logger.warning(f"Path collision detected for series {series.name}, using unique path: {series_final_name}")
                except Exception as e:
                    logger.debug(f"Error checking for path collision: {e}, proceeding with original path")
        
        tv_dir.mkdir(parents=True, exist_ok=True)
        
        # Generate STRM file
        strm_path = tv_dir / f"{episode_filename}.strm"
        
        # Build streaming URL
        base = base_url.rstrip('/')
        stream_url = f"{base}/proxy/vod/episode/{episode.uuid}"
        
        generate_strm_file(strm_path, stream_url)
        
        # Generate NFO file
        nfo_path = tv_dir / f"{episode_filename}.nfo"
        
        # Build metadata with clean title (for metadata matching)
        clean_title = info.get('title', series.name)
        if not clean_title or clean_title == 'Unknown Title':
            clean_title = series.name
        
        metadata = {
            'title': episode.name or f"Episode {episode.episode_number or 0}",
            'plot': episode.description,
            'season': episode.season_number,
            'episode': episode.episode_number,
            'aired': episode.air_date.strftime('%Y-%m-%d') if episode.air_date else None,
            'showtitle': clean_title,  # Clean series title for metadata matching
            'year': info.get('year') or series.year,
            'rating': episode.rating,
            'tmdb_id': episode.tmdb_id,
            'imdb_id': episode.imdb_id,
        }
        
        # Add thumbnail if available (use series logo)
        if series.logo and series.logo.url:
            metadata['thumbnail'] = series.logo.url
        
        generate_nfo_file(nfo_path, 'episode', metadata)
        
        # Update STRMFile record with actual paths used
        # Use update_or_create to handle race condition where record might be deleted
        # by cleanup task between get_or_create and save
        strm_path_str = str(strm_path)
        nfo_path_str = str(nfo_path)
        
        STRMFile.objects.update_or_create(
            content_type=content_type,
            content_id=episode.id,
            defaults={
                'strm_path': strm_path_str,
                'nfo_path': nfo_path_str,
            }
        )
        
        logger.info(f"Generated STRM/NFO files for episode: {episode} (path: tv/{folder_name}/{series_final_name})")
    except Exception as e:
        logger.error(f"Failed to generate STRM files for episode {episode} (ID: {episode.id}): {e}", exc_info=True)
        # Don't raise - we don't want STRM generation failures to break the save operation

