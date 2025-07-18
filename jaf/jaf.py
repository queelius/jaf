from typing import List, Dict, Any, Union, Optional
import logging
from .result_set import JafQuerySet, JafQuerySetError  # Keep for compatibility
from .lazy_streams import FilteredStream, stream
from .jaf_eval import jaf_eval

logger = logging.getLogger(__name__)


class jafError(Exception):
    pass


def jaf(
    data: List[Any],
    query: Union[List, str],
    collection_id: Optional[Any] = None,
    collection_source: Optional[Dict[str, Any]] = None,
) -> JafQuerySet:
    """
    Creates a lazy query set for filtering JSON-like objects based on a JAF query.

    This function creates a JafQuerySet that represents the query without
    immediately evaluating it. The query will be applied when evaluate() is called.
    The data parameter is used to infer collection metadata if collection_source
    is not provided.

    :param data: The list of JSON-like objects (used for metadata only).
    :param query: The JAF query as a list-based AST or a raw JSON string.
    :param collection_id: An optional identifier for the data collection.
    :param collection_source: An optional dictionary describing the data source for later resolution.
    :return: A JafQuerySet containing the query and collection metadata.
    :raises jafError: If the query is invalid.
    """

    if not query:
        raise jafError("No query provided.")
    if not isinstance(data, list):
        raise jafError("Input data must be a list.")

    logger.debug(
        f"Creating lazy query set with {query=} for {len(data)} items. Collection ID: {collection_id}"
    )

    # If no collection_source provided, create one for in-memory data
    if collection_source is None:
        # Note: This is a limitation - in-memory data can't be lazy evaluated
        # In a full implementation, we'd need to store the data somewhere
        logger.warning(
            "No collection_source provided. In-memory data cannot be lazily evaluated."
        )
        collection_source = {"type": "memory", "data": data}
    
    # Wrap the source in a filter if we have a query
    # This ensures that operations like take() work on filtered data
    filtered_source = {
        "type": "filter",
        "query": query,
        "inner_source": collection_source
    }
    
    return JafQuerySet(
        query=query, collection_id=collection_id, collection_source=filtered_source
    )


def jaf_stream(
    source: Union[str, Dict[str, Any], List[Any]],
    query: Union[List, str],
) -> FilteredStream:
    """
    Creates a filtered stream using the new lazy stream architecture.
    
    This is the new API that returns FilteredStream instead of JafQuerySet.
    
    :param source: Data source - can be:
        - A file path string
        - A source descriptor dict
        - A list of JSON values (for compatibility)
    :param query: The JAF query as a list-based AST or a raw JSON string.
    :return: A FilteredStream ready for further operations.
    :raises jafError: If the query is invalid.
    """
    if not query:
        raise jafError("No query provided.")
    
    # Handle legacy list input
    if isinstance(source, list):
        logger.warning(
            "Passing data as a list is deprecated. Consider using a proper source descriptor."
        )
        source_stream = stream({"type": "memory", "data": source})
    else:
        source_stream = stream(source)
    
    logger.debug(f"Creating filtered stream with {query=}")
    
    return source_stream.filter(query)
