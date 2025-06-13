import datetime
import re
import rapidfuzz
import logging
from .path_utils import exists, eval_path, PathValues
from .utils import adapt_jaf_operator

# Set up the logger
logger = logging.getLogger(__name__)

class jafError(Exception):
    pass

class jaf_eval:
    """
    JSON Abstract Filter (JAF) evaluator class. This only applies to a single
    object in the array.
    """
    
    # Special forms that need custom evaluation logic
    special_forms = {
        'path', 'if', 'and', 'or', 'not', 'exists?'
    }
    
    # Regular functions that evaluate all arguments first
    funcs = {
        # predicates with strict type checking
        'eq?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 == x2 and type(x1) == type(x2)),
        'neq?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 != x2 or type(x1) != type(x2)),
        'gt?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 > x2),
        'gte?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 >= x2),
        'lt?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 < x2),
        'lte?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 <= x2),
        'in?': adapt_jaf_operator(3, lambda x1, x2, obj: x1 in x2),
        'starts-with?': adapt_jaf_operator(3, lambda start, value, obj: value.startswith(start)),
        'ends-with?': adapt_jaf_operator(3, lambda end, value, obj: value.endswith(end)),

        # string matching
        'regex-match?': adapt_jaf_operator(3, lambda pattern, value, obj: re.match(pattern, value) is not None),
        'close-match?': adapt_jaf_operator(3, lambda x1, x2, obj: rapidfuzz.fuzz.ratio(x1, x2) > 80),
        'partial-match?': adapt_jaf_operator(3, lambda x1, x2, obj: rapidfuzz.fuzz.partial_ratio(x1, x2) > 80),

        # value extractors
        'length': adapt_jaf_operator(2, lambda x, obj: len(x)),
        'type': adapt_jaf_operator(2, lambda x, obj: type(x).__name__),
        'keys': adapt_jaf_operator(2, lambda x, obj: list(x.keys())),

        # datetime functions
        'now': adapt_jaf_operator(1, lambda obj: datetime.datetime.now()),
        'date': adapt_jaf_operator(2, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d')),
        'datetime': adapt_jaf_operator(2, lambda x, obj: datetime.datetime.strptime(x, '%Y-%m-%d %H:%M:%S')),
        'date-diff': adapt_jaf_operator(3, lambda date1, date2, obj: date1 - date2),
        'days': adapt_jaf_operator(2, lambda datediff, obj: datediff.days),
        'seconds': adapt_jaf_operator(2, lambda datediff, obj: datediff.seconds),

        # string functions
        'lower-case': adapt_jaf_operator(2, lambda s, obj: s.lower()),
        'upper-case': adapt_jaf_operator(2, lambda s, obj: s.upper()),
    }

    @staticmethod
    def eval(query, obj):
        """
        Evaluates the query against the provided object.

        :param query: The query AST as a list.
        :param obj: The dictionary object to evaluate.
        :return: Result of the evaluation.
        """
        # Handle non-list values (literals)
        if not isinstance(query, list):
            return query
            
        if not query:
            raise ValueError("Invalid query format.")
        
        # Get the operator (first element)
        op = query[0]
        args = query[1:]
        
        logger.debug(f"Evaluating operator: '{op}' with args: {args}")
        
        # Handle special forms first
        if op in jaf_eval.special_forms:
            return jaf_eval._eval_special_form(op, args, obj)
        
        # Handle regular functions
        if op in jaf_eval.funcs:
            return jaf_eval._eval_function(op, args, obj)
        
        raise ValueError(f"Unknown operator: {op}")
    
    @staticmethod
    def _eval_special_form(op, args, obj):
        """Handle special forms that need custom evaluation logic"""
        
        if op == 'path':
            if len(args) != 1:
                raise ValueError(f"'path' expects 1 argument, got {len(args)}")
            path_expr = args[0]
            if not isinstance(path_expr, list):
                raise ValueError("path argument must be a list of path components")

            # Validate each component of the path expression
            known_path_ops = {"key", "index", "indices", "slice", "regex_key", "wc_level", "wc_recursive"}
            for component in path_expr:
                if not isinstance(component, list):
                    raise ValueError("Path component must be a list")
                if not component:
                    raise ValueError("Path component cannot be empty")
                if not isinstance(component[0], str):
                    raise ValueError("Path component operation must be a string")
                if component[0] not in known_path_ops:
                    raise ValueError(f"Unknown path operation: {component[0]}")
            
            return eval_path(path_expr, obj)
        
        elif op == 'exists?':
            if len(args) != 1:
                raise ValueError(f"'exists?' expects 1 argument, got {len(args)}")
    
            # The argument should be a path expression like ["path", ["user", "email"]]
            arg = args[0]
            if isinstance(arg, list) and len(arg) == 2 and arg[0] == "path":
                # Extract the path components directly
                path_components = arg[1]
                if not isinstance(path_components, list):
                    raise ValueError("path argument must be a list of path components")
                return exists(path_components, obj)
            else:
                raise ValueError("exists? argument must be a path expression")
        
        elif op == 'if':
            if len(args) != 3:
                raise ValueError(f"'if' expects 3 arguments, got {len(args)}")
            cond_expr, true_expr, false_expr = args
            
            # Evaluate condition
            cond_result = jaf_eval.eval(cond_expr, obj)
            
            # Return appropriate branch without evaluating the other
            if cond_result:
                return jaf_eval.eval(true_expr, obj)
            else:
                return jaf_eval.eval(false_expr, obj)
        
        elif op == 'and':
            # Short-circuit evaluation - stop at first falsy value
            for arg in args:
                result = jaf_eval.eval(arg, obj)
                if not result:
                    return False
            return True
        
        elif op == 'or':
            # Short-circuit evaluation - stop at first truthy value
            for arg in args:
                result = jaf_eval.eval(arg, obj)
                if result:
                    return True
            return False
        
        elif op == 'not':
            if len(args) != 1:
                raise ValueError(f"'not' expects 1 argument, got {len(args)}")
            result = jaf_eval.eval(args[0], obj)
            return not result
        
        else:
            raise ValueError(f"Unknown special form: {op}")
    
    @staticmethod
    def _eval_function(op, args, obj):
        """Handle regular functions that evaluate all arguments first"""
        
        func, nargs = jaf_eval.funcs[op]
        
        # Check argument count for non-variadic functions
        if nargs != -1 and len(args) != nargs - 1:
            raise ValueError(f"'{op}' expects {nargs-1} arguments, got {len(args)}")
        
        # Evaluate all arguments
        eval_args = []
        for arg in args:
            if isinstance(arg, list):
                val = jaf_eval.eval(arg, obj)
                eval_args.append(val)
            else:
                eval_args.append(arg)
        
        # Call the function with type error handling for predicates
        try:
            result = func(*eval_args, obj=obj)
            logger.debug(f"Result of '{op}': {result}")
            return result
        except Exception as e:
            logger.debug(f"Error evaluating '{op}' with args {eval_args}: {e}")
            # For predicates (functions ending with '?'), return False on type errors
            if op.endswith('?'):
                return False
            else:
                # For non-predicates, re-raise the exception
                raise e
