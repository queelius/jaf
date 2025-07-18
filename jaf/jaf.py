from typing import List, Dict, Any, Union, Optional
import logging
from .result_set import JafQuerySet, JafQuerySetError
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

    return JafQuerySet(
        query=query, collection_id=collection_id, collection_source=collection_source
    )
