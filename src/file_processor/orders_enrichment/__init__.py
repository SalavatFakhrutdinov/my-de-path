from file_processor.orders_enrichment.main import (
    main,
    run_etl_pipeline,
    parse_arguments,
)
from file_processor.orders_enrichment.extractor import (
    extract_users,
    extract_orders,
    extract_users_streaming,
    extract_orders_streaming,
    get_user_ids,
)
from file_processor.orders_enrichment.validator import (
    validate_order,
    filter_by_watermark,
    get_user_ids as get_user_ids_validator,
)
from file_processor.orders_enrichment.transformer import (
    build_user_map,
    join_users_orders,
    enrich_orders_with_user_data,
)

__version__ = "1.0.0"

__all__ = [
    "__version__",
    # Main
    "main",
    "run_etl_pipeline",
    "parse_arguments",
    # Extractor
    "extract_users",
    "extract_orders",
    "extract_users_streaming",
    "extract_orders_streaming",
    "get_user_ids",
    # Validator
    "validate_order",
    "filter_by_watermark",
    # Transformer
    "build_user_map",
    "join_users_orders",
    "enrich_orders_with_user_data",
]
