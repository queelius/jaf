from typing import List, Dict, Any, Union, Optional
import logging
from .result_set import JafResultSet, JafResultSetError
from .jaf_eval import jaf_eval

logger = logging.getLogger(__name__)

class jafError(Exception):
    pass

def jaf(data: List[Any], query: Union[List, str], collection_id: Optional[Any] = None) -> JafResultSet:
    """
    Filter a list of JSON values based on a query AST (nested lists) or DSL
    string. Only dictionary items in the data list are actively filtered.

    Args:
        data: List of JSON values to filter.
        query: Nested list/dict AST or a DSL string representing the filter.
        collection_id: Optional identifier for the data collection.

    Returns:
        JafResultSet containing indices of items that satisfy the query.

    Raises:
        jafError: If query is invalid or evaluation fails.
    """

    if not query:
        raise jafError("No query provided.")
    if not isinstance(data, list):
        # This check is simplified as data is List[Any]
        raise jafError("Input data must be a list.")


    logger.debug(f"Applying {query=} to {len(data)} items. Collection ID: {collection_id}")
    try:
        results_indices = []
        for i, item in enumerate(data): # Renamed obj to item
            if isinstance(item, dict): # Only attempt to evaluate on dictionaries
                logger.debug(f"Evaluating {query=} against dictionary item at index {i}: {item=}.")
                result = jaf_eval.eval(query, item) # Assuming jaf_eval.eval exists and works
                if isinstance(result, bool):
                    if result:
                        logger.debug(f"Item at index {i} satisfied the query.")
                        results_indices.append(i)
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


    return JafResultSet(indices=results_indices, collection_size=len(data), collection_id=collection_id)