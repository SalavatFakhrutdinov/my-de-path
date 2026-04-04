from file_processor.user_filter.main import main, run_application, parse_arguments
from file_processor.user_filter.validator import (
    validate_user,
    filter_adults,
    sort_by_age,
)
from file_processor.user_filter.transformations import (
    normalize_name,
    add_age_group,
    apply_transformations,
    batch_transform,
)

__version__ = "1.0.0"

__all__ = [
    "__version__",
    # Main
    "main",
    "run_application",
    "parse_arguments",
    # Validator
    "validate_user",
    "filter_adults",
    "sort_by_age",
    # Transformations
    "normalize_name",
    "add_age_group",
    "apply_transformations",
    "batch_transform",
]
