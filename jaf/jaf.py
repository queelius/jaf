from typing import List, Dict, Any, Union
import logging

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

