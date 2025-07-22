import logging
from celery import shared_task
import tempfile
import os
import json
import zipfile
import shutil
from pathlib import Path
from urllib.parse import urlparse
from django.conf import settings
from django.core.files.base import ContentFile, File
from django.core.files.storage import default_storage
from django.core.management import call_command
from django.utils import timezone
from django.apps import apps
from django.db import models

from django_multitenant.utils import unset_current_tenant

from superapp.apps.backups.models.backup import Backup
from superapp.apps.multi_tenant.middleware import set_current_tenant

logger = logging.getLogger(__name__)


def extract_media_files_from_fixture(fixture_data):
    """
    Extract media file paths from fixture data by examining FileField and ImageField values.
    
    Args:
        fixture_data: Parsed JSON fixture data
        
    Returns:
        Set of media file paths referenced in the fixture
    """
    media_files = set()
    
    if not isinstance(fixture_data, list):
        return media_files
    
    for obj in fixture_data:
        if not isinstance(obj, dict) or 'model' not in obj or 'fields' not in obj:
            continue
            
        model_name = obj['model']
        fields = obj['fields']
        
        try:
            # Get the actual model class
            app_label, model_class_name = model_name.split('.')
            model_class = apps.get_model(app_label, model_class_name)
            
            # Check each field in the fixture
            for field_name, field_value in fields.items():
                if not field_value:
                    continue
                    
                try:
                    # Get the field from the model
                    field = model_class._meta.get_field(field_name)
                    
                    # Check if it's a FileField or ImageField
                    if isinstance(field, (models.FileField, models.ImageField)):
                        # Extract the file path (remove any URL prefixes)
                        if isinstance(field_value, str) and field_value.strip():
                            # Handle both relative paths and full URLs
                            if field_value.startswith('http'):
                                parsed_url = urlparse(field_value)
                                file_path = parsed_url.path.lstrip('/')
                            else:
                                file_path = field_value.lstrip('/')
                            
                            # Remove media URL prefix if present
                            if hasattr(settings, 'MEDIA_URL') and settings.MEDIA_URL:
                                media_url = settings.MEDIA_URL.strip('/')
                                if file_path.startswith(media_url + '/'):
                                    file_path = file_path[len(media_url) + 1:]
                            
                            if file_path:
                                media_files.add(file_path)
                                
                except Exception as e:
                    # Field might not exist or be accessible, skip it
                    logger.debug(f"Could not process field {field_name} in {model_name}: {e}")
                    continue
                    
        except Exception as e:
            logger.debug(f"Could not process model {model_name}: {e}")
            continue
    
    return media_files


def copy_media_files_to_backup(media_files, backup_dir):
    """
    Copy media files to the backup directory, preserving the directory structure.
    Handles both local filesystem and remote storage (S3, etc.) via Django's storage system.
    
    Args:
        media_files: Set of media file paths to copy
        backup_dir: Directory where media files should be copied
        
    Returns:
        Dict with 'copied' and 'missing' file lists
    """
    copied_files = []
    missing_files = []
    
    backup_media_dir = Path(backup_dir) / 'media'
    backup_media_dir.mkdir(parents=True, exist_ok=True)
    
    for file_path in media_files:
        dest_path = backup_media_dir / file_path
        
        try:
            # Check if file exists in the storage backend (local, S3, etc.)
            if default_storage.exists(file_path):
                # Create destination directory if it doesn't exist
                dest_path.parent.mkdir(parents=True, exist_ok=True)
                
                # Open the file from storage (works with S3, local, etc.)
                with default_storage.open(file_path, 'rb') as source_file:
                    # Write to local backup directory
                    with open(dest_path, 'wb') as dest_file:
                        # Copy in chunks to handle large files efficiently
                        for chunk in iter(lambda: source_file.read(8192), b''):
                            dest_file.write(chunk)
                
                copied_files.append(file_path)
                logger.debug(f"Copied media file from storage: {file_path}")
                
            else:
                missing_files.append(file_path)
                logger.warning(f"Media file not found in storage: {file_path}")
                
        except Exception as e:
            missing_files.append(file_path)
            logger.error(f"Error copying media file {file_path}: {e}")
    
    return {
        'copied': copied_files,
        'missing': missing_files
    }


def create_backup_archive(json_file_path, backup_dir, archive_name):
    """
    Create a zip archive containing the JSON backup and media files.
    
    Args:
        json_file_path: Path to the JSON fixture file
        backup_dir: Directory containing media files
        archive_name: Name for the zip archive
        
    Returns:
        Path to the created zip archive
    """
    archive_path = Path(backup_dir) / f"{archive_name}.zip"
    
    with zipfile.ZipFile(archive_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
        # Add the JSON file to the root of the archive with standardized name
        zipf.write(json_file_path, "backup.json")
        
        # Add all media files in a "media" folder within the archive
        media_dir = Path(backup_dir) / 'media'
        if media_dir.exists():
            for file_path in media_dir.rglob('*'):
                if file_path.is_file():
                    # Calculate relative path from media directory
                    relative_path = file_path.relative_to(media_dir)
                    # Ensure all media files are placed inside "media/" folder in the archive
                    archive_media_path = f"media/{relative_path}"
                    zipf.write(file_path, archive_media_path)
                    logger.debug(f"Added to archive: {archive_media_path}")
        
        # Add a manifest file with backup information
        manifest = {
            'backup_type': 'data_with_media',
            'created_at': timezone.now().isoformat(),
            'json_file': "backup.json",
            'media_directory': 'media/',
            'format_version': '1.0'
        }
        
        manifest_json = json.dumps(manifest, indent=2)
        zipf.writestr('backup_manifest.json', manifest_json)
    
    return archive_path


def get_models_for_backup_type(backup_type):
    """
    Get the list of models to backup based on the backup type.

    Args:
        backup_type: The backup type string

    Returns:
        List of model names to backup, or '*' for all models
    """
    # Get backup types from settings
    backup_types = getattr(settings, 'BACKUPS', {}).get('BACKUP_TYPES', {})

    # If the backup type doesn't exist in settings, default to all models
    if backup_type not in backup_types:
        return '*'

    # Get the models from the backup type configuration
    backup_type_config = backup_types.get(backup_type, {})
    return backup_type_config.get('models', '*')

@shared_task(
    bind=True,
    name="backups.process_backup",
    max_retries=3,
    default_retry_delay=60,
)
def process_backup(self, backup_pk):
    """
    Celery task to create a backup for a tenant.

    Args:
        backup_pk: The primary key of the Backup object to be processed

    Returns:
        The ID of the created backup
    """
    try:
        # Clear any existing tenant first
        unset_current_tenant()

        backup = Backup.objects.get(pk=backup_pk)
        tenant = backup.tenant
        logger.info(f"Processing backup with ID: {backup_pk} for tenant: {tenant} of type: {backup.type}")

        # Set the tenant context
        if tenant:
            set_current_tenant(tenant)

        backup.started_at = timezone.now()
        backup.save(update_fields=['started_at'])

        # Get models to backup based on backup type
        models_to_backup = get_models_for_backup_type(backup.type)

        # If models_to_backup is '*', backup all models
        if models_to_backup == '*':
            # Get all installed models
            args = []
            for app_config in apps.get_app_configs():
                for model in app_config.get_models():
                    model_name = f"{model._meta.app_label}.{model._meta.model_name}"
                    args.append(model_name)
        else:
            # Use the specific models defined in the backup type
            args = models_to_backup

        print(args)
        
        # Create a temporary directory for the backup process
        with tempfile.TemporaryDirectory() as temp_dir:
            temp_file_path = os.path.join(temp_dir, 'backup.json')

            # Set up options for the tenant_dumpdata command
            options = {
                'output': temp_file_path,
                'format': 'json',
                'indent': 2,
                'database': 'default',
            }
            if tenant:
                options['tenant_pk'] = tenant.pk

            # If backup type is 'tenant_models', use export_organization command
            # For other backup types, use tenant_dumpdata
            if tenant:
                call_command('tenant_dumpdata', *args, **options)
            else:
                call_command('dumpdata', *args, **options)

            # Read the generated JSON file to extract media file references
            with open(temp_file_path, 'r') as f:
                fixture_data = json.load(f)
            
            # Extract media files referenced in the fixture
            media_files = extract_media_files_from_fixture(fixture_data)
            logger.info(f"Found {len(media_files)} media files referenced in backup")
            
            # Copy media files to backup directory
            media_copy_result = copy_media_files_to_backup(media_files, temp_dir)
            logger.info(f"Copied {len(media_copy_result['copied'])} media files, "
                       f"{len(media_copy_result['missing'])} files were missing")
            
            # Create backup filename
            backup.finished_at = timezone.now()
            if tenant:
                archive_name = f'backup_{tenant.pk}_{backup.type}_{backup.finished_at.strftime("%Y%m%d_%H%M%S")}'
            else:
                archive_name = f'backup_{backup.type}_{backup.finished_at.strftime("%Y%m%d_%H%M%S")}'
            
            # Create zip archive with JSON data and media files
            archive_path = create_backup_archive(temp_file_path, temp_dir, archive_name)
            
            # Save the zip archive as the backup file
            with open(archive_path, 'rb') as archive_file:
                backup.file.save(
                    name=f'{archive_name}.zip',
                    content=File(archive_file),
                    save=True
                )
            
            backup.done = True
            backup.save(update_fields=['file', 'done', 'finished_at'])
            
            # Log backup statistics
            logger.info(f"Backup completed: {archive_name}.zip")
            logger.info(f"Media files: {len(media_copy_result['copied'])} copied, "
                       f"{len(media_copy_result['missing'])} missing")
            if media_copy_result['missing']:
                logger.warning(f"Missing media files: {media_copy_result['missing']}")

        # Clean up tenant context
        unset_current_tenant()

        return backup.id
    except Exception as exc:
        self.retry(exc=exc)
        return None
