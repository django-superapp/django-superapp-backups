# Generated by Django 5.1.9 on 2025-05-22 13:02

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('backups', '0008_restore_cleanup_existing_data_restore_exclude_models'),
    ]

    operations = [
        migrations.RemoveField(
            model_name='restore',
            name='exclude_models',
        ),
        migrations.AddField(
            model_name='restore',
            name='exclude_models_from_import',
            field=models.JSONField(default=list, verbose_name='Exclude models from import'),
        ),
    ]
