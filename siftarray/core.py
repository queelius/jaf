from typing import List, Dict, Any, Union
import logging
from siftarray.dsl.parser import dsl_parser
from siftarray.ast.ast_nodes import Condition, LogicalOperation, FilterError

# Configure logging
logging.basicConfig(level=logging.ERROR)
logger = logging.getLogger(__name__)

class FilterQuery:
    """Class to parse and evaluate filter queries represented as an AST."""

    def __init__(self, query: Union[List, Dict, str], is_dsl: bool = False):
        """
        Initialize the FilterQuery.

        Args:
            query: The filter query, either as a list/dict AST or a DSL string.
            is_dsl: Indicates if the query is a DSL string.
        """
        self.query = query
        if is_dsl:
            self.ast = self.parse_dsl(query)
        else:
            self.ast = self.parse_ast(query)

    def parse_ast(self, query: Union[List, Dict]) -> Any:
        """Parse the query AST into internal AST objects."""
        if isinstance(query, list):
            if not query:
                raise FilterError("Empty query list.")
            operator = query[0]
            if isinstance(operator, str):
                operator_lower = operator.lower()
                if operator_lower in LogicalOperation.logical_operators:
                    operands = [self.parse_ast(subquery) for subquery in query[1:]]
                    return LogicalOperation(operator_lower, operands)
            # If not a logical operator, treat as condition
            if len(query) != 3:
                raise FilterError(f"Invalid condition format: {query}")
            field, operator, value = query
            if not isinstance(operator, str):
                raise FilterError(f"Invalid operator type: {type(operator)} in condition.")
            return Condition(field, operator.lower(), value)
        elif isinstance(query, dict):
            # Only allow one key in the dictionary (the operator)
            if not query:
                raise FilterError("Empty query dictionary.")
            if len(query) != 1:
                raise FilterError("Query dictionary must have exactly one key (the operator).")
            operator, operands = next(iter(query.items()))
            if not isinstance(operator, str):
                raise FilterError(f"Invalid logical operator type: {type(operator)}")
            operator_lower = operator.lower()
            if operator_lower not in LogicalOperation.logical_operators:
                raise FilterError(f"Unsupported logical operator in dict: {operator_lower}")
            if not isinstance(operands, list):
                raise FilterError(f"Operands must be a list for operator '{operator_lower}'.")
            parsed_operands = [self.parse_ast(subquery) for subquery in operands]
            return LogicalOperation(operator_lower, parsed_operands)
        else:
            raise FilterError(f"Invalid query type: {type(query)}")

    def parse_dsl(self, query: str) -> Any:
        """Parse the DSL string into internal AST objects."""
        try:
            ast = dsl_parser.parse(query)
            return ast
        except Exception as e:
            logger.error(f"Failed to parse DSL query: {e}")
            raise FilterError(f"Failed to parse DSL query: {e}")

    def evaluate(self, repo: Dict) -> bool:
        """Evaluate the parsed AST against a repository."""
        return self.ast.evaluate(repo)

def sift_array(repos: List[Dict], query: Union[List, Dict, str], is_dsl: bool = False) -> List[Dict]:
    """
    Filter repositories based on custom conditions using an AST-based or DSL query.

    Args:
        repos: List of repository dictionaries.
        query: Nested list/dict AST or a DSL string representing the filter conditions and logical operators.
        is_dsl: If True, treat the query as a DSL string.

    Returns:
        List of repositories matching the query.

    Raises:
        FilterError: If query is invalid or evaluation fails.
    """
    if not query:
        raise FilterError("No query provided.")

    try:
        filter_query = FilterQuery(query, is_dsl=is_dsl)
    except FilterError as e:
        logger.error(f"Failed to parse query: {e}")
        raise

    filtered_repos = []

    for repo in repos:
        try:
            if filter_query.evaluate(repo):
                filtered_repos.append(repo)
        except FilterError as e:
            logger.error(f"Error evaluating repo {repo.get('id', 'unknown')}: {e}")
            # Depending on requirements, you might choose to continue or halt
            continue

    return filtered_repos
