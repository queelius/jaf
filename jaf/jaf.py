from typing import List, Dict, Any, Union, Optional
import logging
from .result_set import JafResultSet, JafResultSetError
from .jaf_eval import jaf_eval

logger = logging.getLogger(__name__)

class jafError(Exception):
    pass

def jaf(data: List[Any], query: Union[List, str], collection_id: Optional[Any] = None, collection_source: Optional[Dict[str, Any]] = None) -> JafResultSet:
    """
    Filters a list of JSON-like objects based on a JAF query.

    This function evaluates the provided query against each item in the data
    list, which should contain JSON-serializable objects. The query can be
    either a nested list/dict structure representing the AST or a DSL string.
    Only items that are dictionaries will be considered for matching.

    :param data: The list of JSON-like objects to filter.
    :param query: The JAF query as a list-based AST or a raw JSON string.
    :param collection_id: An optional identifier for the data collection.
    :param collection_source: An optional dictionary describing the data source for later resolution.
    :return: A JafResultSet containing the indices of matching objects.
    :raises jafError: If the query is invalid or an evaluation error occurs.
    """

    if not query:
        raise jafError("No query provided.")
    if not isinstance(data, list):
        # This check is simplified as data is List[Any]
        raise jafError("Input data must be a list.")


    logger.debug(f"Applying {query=} to {len(data)} items. Collection ID: {collection_id}")
    try:
        matching_indices = []
        for i, item in enumerate(data): # Renamed obj to item
            if isinstance(item, dict): # Only attempt to evaluate on dictionaries
                logger.debug(f"Evaluating {query=} against dictionary item at index {i}: {item=}.")
                result = jaf_eval.eval(query, item) # Assuming jaf_eval.eval exists and works
                if isinstance(result, bool):
                    if result:
                        logger.debug(f"Item at index {i} satisfied the query.")
                        matching_indices.append(i)
                    else:
                        logger.debug(f"Item at index {i} did not satisfy the query.")
                else:
                    logger.warning(f"Query returned non-boolean value for item at index {i}: {result}. Item does not match.")
            else:
                # Non-dictionary items are not processed by path-based queries, so they effectively don't match.
                logger.debug(f"Skipping non-dictionary item at index {i}: {type(item)} as it cannot match path-based queries.")
    except jafError as e: # Errors from jaf_eval
        logger.error(f"Failed to evaluate query: {e}", exc_info=True)
        raise
    except Exception as e_unexp: # Catch other unexpected errors during eval
        logger.error(f"Unexpected error during JAF evaluation: {e_unexp}", exc_info=True)
        raise jafError(f"Unexpected JAF evaluation error: {e_unexp}")


    return JafResultSet(
        indices=matching_indices,
        collection_size=len(data),
        collection_id=collection_id,
        collection_source=collection_source,
        query=query
    )