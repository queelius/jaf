from typing import List, Dict, Any, Union, Optional
import logging
from .result_set import JafResultSet, JafResultSetError

logger = logging.getLogger(__name__)

logger = logging.getLogger(__name__)

class jafError(Exception):
    pass

def jaf(data: List[Dict], query: Union[List, str]) -> List[int]:
    """
    Filter JSON arrays of objects based on a query AST (nested lists) or DSL
    string.

    Args:
        data: List of dictionaries to filter.
        query: Nested list/dict AST or a DSL string representing the filter.

    Returns:
        List of indices of objects that satisfy the query.

    Raises:
        jafError: If query is invalid or evaluation fails.
    """
    from .jaf_eval import jaf_eval

    if not query:
        raise jafError("No query provided.")

    logger.debug(f"Applying {query=} to {len(data)} objects.")
    try:
        results = []
        for i, obj in enumerate(data):
            if isinstance(obj, dict):
                logger.debug(f"Evaluating {query=} against {obj=}.")
                result = jaf_eval.eval(query, obj)
                if isinstance(result, bool):
                    if result:
                        logger.debug("Object satisfied the query.")
                        results.append(i)
                    else:
                        logger.debug("Object did not satisfy the query.")
                else:
                    logger.warning(f"Query returned non-boolean value: {result}. Skipping object.")
            else:
                logger.error(f"Skipping non-dictionary object: {obj}.")
    except jafError as e:
        logger.error(f"Failed to evaluate query: {e}")
        raise

    return results



def jaf(data: List[Dict], query: Union[List, str], collection_id: Optional[Any] = None) -> JafResultSet: # Modified return type and added collection_id
    """
    Filter JSON arrays of objects based on a query AST (nested lists) or DSL
    string.

    Args:
        data: List of dictionaries to filter.
        query: Nested list/dict AST or a DSL string representing the filter.
        collection_id: Optional identifier for the data collection.

    Returns:
        JafResultSet containing indices of objects that satisfy the query.

    Raises:
        jafError: If query is invalid or evaluation fails.
    """
    from .jaf_eval import jaf_eval

    if not query:
        raise jafError("No query provided.")
    if not isinstance(data, list):
        # This check could be more robust (e.g. check if all elements are dicts)
        # but for now, we ensure it's a list as per type hint.
        raise jafError("Input data must be a list of dictionaries.")


    logger.debug(f"Applying {query=} to {len(data)} objects. Collection ID: {collection_id}")
    try:
        results_indices = []
        for i, obj in enumerate(data):
            if isinstance(obj, dict):
                logger.debug(f"Evaluating {query=} against {obj=}.")
                result = jaf_eval.eval(query, obj)
                if isinstance(result, bool):
                    if result:
                        logger.debug("Object satisfied the query.")
                        results_indices.append(i)
                    else:
                        logger.debug("Object did not satisfy the query.")
                else:
                    logger.warning(f"Query returned non-boolean value: {result}. Skipping object.")
            else:
                # According to type hints, data should be List[Dict].
                # If we encounter non-dict, it's an issue with input data.
                logger.error(f"Skipping non-dictionary object at index {i}: {type(obj)}. Expected dict.")
                # Depending on strictness, you might raise an error here or just skip.
                # For now, skipping.
    except jafError as e:
        logger.error(f"Failed to evaluate query: {e}")
        raise
    except Exception as e_unexp: # Catch unexpected errors during eval
        logger.error(f"Unexpected error during JAF evaluation: {e_unexp}", exc_info=True)
        raise jafError(f"Unexpected JAF evaluation error: {e_unexp}")


    return JafResultSet(indices=results_indices, collection_size=len(data), collection_id=collection_id)

