"""Catalog parsing utilities for US Census Bureau connector."""

from .constants import CURRENT_YEAR, PRIORITY_DATASETS


def extract_dataset_type(dataset):
    """Extract dataset type from c_dataset array."""
    if 'c_dataset' in dataset and isinstance(dataset['c_dataset'], list):
        dataset_path = '/'.join(dataset['c_dataset'])
        for pattern in PRIORITY_DATASETS:
            if pattern in dataset_path.lower():
                return pattern, dataset_path
    return None, None


def should_include_dataset(dataset):
    """Check if dataset should be included based on type, vintage, and availability."""
    dataset_type, _ = extract_dataset_type(dataset)
    if not dataset_type:
        return False

    vintage = dataset.get('c_vintage')
    if vintage:
        try:
            year = int(vintage)
            min_year = PRIORITY_DATASETS[dataset_type]['min_year']
            max_year = PRIORITY_DATASETS[dataset_type].get('max_year', CURRENT_YEAR)
            if year < min_year or year > max_year:
                return False
        except (ValueError, TypeError):
            pass

    if not dataset.get('c_isAvailable', True):
        return False

    return True


def parse_dataset_metadata(dataset):
    """Extract metadata from dataset entry."""
    dataset_type, dataset_path = extract_dataset_type(dataset)

    api_endpoint = None
    if 'distribution' in dataset:
        for dist in dataset['distribution']:
            if dist.get('format') == 'API':
                api_endpoint = dist.get('accessURL')
                break

    return {
        'dataset_id': dataset.get('identifier', ''),
        'dataset_type': dataset_type,
        'dataset_path': dataset_path,
        'vintage': dataset.get('c_vintage'),
        'title': dataset.get('title', ''),
        'description': dataset.get('description', '')[:500] if dataset.get('description') else '',
        'api_endpoint': api_endpoint,
        'variables_link': dataset.get('c_variablesLink'),
        'geography_link': dataset.get('c_geographyLink'),
        'groups_link': dataset.get('c_groupsLink'),
        'examples_link': dataset.get('c_examplesLink'),
        'is_microdata': dataset.get('c_isMicrodata', False),
        'is_aggregate': dataset.get('c_isAggregate', False),
        'is_cube': dataset.get('c_isCube', False),
        'modified': dataset.get('modified', ''),
        'priority': PRIORITY_DATASETS.get(dataset_type, {}).get('priority', 99)
    }
