from typing import List, Dict, Any, Union
import logging
#from .dsl.parser import dsl_parser
from .jaf_eval import jaf_eval
import json

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

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

    try:
        results = []
        for obj in data:
            if isinstance(obj, dict):
                print(f"Evaluating {query=} against object:")
                print(json.dumps(obj, indent=2))
                result = jaf_eval.eval(query, obj)
                if type(result) == bool:
                    if result:
                        results.append(obj)
                    else:
                        print("Object did not satisfy the query.")
                else:
                    print(f"Retuned a non-boolean value: {result}")

            else:
                print("Skipping non-dictionary object.")
    except jafError as e:
        logger.error(f"Failed to evaluate query: {e}")
        raise

    return results

