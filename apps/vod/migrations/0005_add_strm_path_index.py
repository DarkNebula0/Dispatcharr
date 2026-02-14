# Generated migration to add index on STRMFile.strm_path

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('vod', '0004_strmfile'),
    ]

    operations = [
        migrations.AddIndex(
            model_name='strmfile',
            index=models.Index(fields=['strm_path'], name='vod_strmfile_strm_path_idx'),
        ),
    ]






