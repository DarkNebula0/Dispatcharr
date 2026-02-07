"""
Django signals for VOD models.

Automatically generates STRM and NFO files when movies or episodes are created or updated.
"""
import logging
from django.db.models.signals import post_save
from django.dispatch import receiver
from django.db import transaction
from .models import Movie, Episode
from .strm_generator import generate_movie_strm, generate_episode_strm

logger = logging.getLogger(__name__)


@receiver(post_save, sender=Movie)
def movie_post_save(sender, instance, created, **kwargs):
    """
    Signal handler for Movie post_save.
    
    Generates STRM and NFO files after a movie is saved.
    Uses transaction.on_commit to ensure the database transaction is complete
    before generating files.
    
    Args:
        sender: The model class (Movie)
        instance: The actual instance being saved
        created: Boolean indicating if this is a new record
        **kwargs: Additional keyword arguments
    """
    # Only generate STRM files if the movie has at least one M3U relation
    # (meaning it's actually available for streaming)
    if instance.m3u_relations.exists():
        # Capture instance ID instead of instance to avoid stale references in long transactions
        movie_id = instance.id
        # Use transaction.on_commit to ensure the save is fully committed
        # before generating files
        transaction.on_commit(lambda: generate_movie_strm(Movie.objects.get(id=movie_id)))
        logger.info(f"Queued STRM generation for movie: {instance.name} (created={created})")
    else:
        logger.debug(f"Skipping STRM generation for movie {instance.name} - no M3U relations")


@receiver(post_save, sender=Episode)
def episode_post_save(sender, instance, created, **kwargs):
    """
    Signal handler for Episode post_save.
    
    Generates STRM and NFO files after an episode is saved.
    Uses transaction.on_commit to ensure the database transaction is complete
    before generating files.
    
    Args:
        sender: The model class (Episode)
        instance: The actual instance being saved
        created: Boolean indicating if this is a new record
        **kwargs: Additional keyword arguments
    """
    # Only generate STRM files if the episode has at least one M3U relation
    # (meaning it's actually available for streaming)
    if instance.m3u_relations.exists():
        # Capture instance ID instead of instance to avoid stale references in long transactions
        episode_id = instance.id
        # Use transaction.on_commit to ensure the save is fully committed
        # before generating files
        transaction.on_commit(lambda: generate_episode_strm(Episode.objects.get(id=episode_id)))
        logger.info(f"Queued STRM generation for episode: {instance} (created={created})")
    else:
        logger.debug(f"Skipping STRM generation for episode {instance} - no M3U relations")

