from typing import List, Dict, Any, Union
import logging
from .jaf_eval import jaf_eval
from .path import PathValue, path_values, path_values_ast, has_path, has_path_value, has_path_value_type, has_path_components
from .dsl.parse import parse_dsl

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.DEBUG)

class jafError(Exception):
    pass

def jaf(data: List[Dict], query: Union[List, str]) -> List[Dict]:
    """
    Filter JSON arrays of objects based on a query AST (nested lists) or DSL
    string.

    Args:
        data: List of dictionaries to filter.
        query: Nested list/dict AST or a DSL string representing the filter.

    Returns:
        List of objects (dictionaries) that satisfy the query.

    Raises:
        jafError: If query is invalid or evaluation fails.
    """
    if not query:
        raise jafError("No query provided.")

    if isinstance(query, str):
        # Parse the DSL string into an AST
        try:
            query = parse_dsl(query)
        except Exception as e:
            logger.error(f"Failed to parse DSL query: {e}")
            raise

    logger.debug(f"Applying {query=} to {len(data)} objects.")
    value_results = {}
    try:
        results = []
        for i,  obj in enumerate(data):
            if isinstance(obj, dict):
                logger.debug(f"Evaluating {query=} against {obj=}.")
                result = jaf_eval.eval(query, obj)
                if type(result) == bool:
                    if result:
                        logger.debug("Object satisfied the query.")
                        results.append(i)
                    else:
                        logger.debug("Object did not satisfy the query.")
                else:
                    logger.debug(f"Retuned a non-boolean value: {result}. Storing in value-results.")
                    value_results[i] = result
            else:
                logger.error("Skipping non-dictionary object: {obj}.")
    except jafError as e:
        logger.error(f"Failed to evaluate query: {e}")
        raise

    return {"matching-indices" : results, "value-results": value_results}

