# import itertools
# import logging
# from .path import PathValue, OpValue
# from typing import Any, List, Type, Callable, Optional, Tuple
# import inspect

# logger = logging.getLogger(__name__)


# def func_namer(func):
#     if func.__name__ == "<lambda>":
#         body = inspect.getsource(func).split(":")[1].strip()
#         args = inspect.getfullargspec(func).args
#         args = ', '.join(args)
#         return f"λ {args} ↦ {body}"
#     else:
#         return func.__name__


# #def wrap(n, func, func_name=None):
# def wrap(func: Callable,
#          func_name: Optional[str] = None):

#     if not callable(func):
#         raise TypeError("func must be a callable")

#     if func_name is None:
#         func_name = func_namer(func)

#     logger.debug(f"wrap called with func={func}, func_name={func_name}")

#     def _wrapper(*args, obj):

#         results = []

#         logger.debug(f"args: {args}")

#         # if isinstance(args, PathValue):
#         #     logger.debug(f"path-value with args {args}")
#         #     val = func(args.value, obj)
#         #     logger.debug(f"apply {func_name} to {args.value} -> {val}")
#         #     op_val = OpValue(value=val, derived_from=args.value, composition=[func_name])
#         #     logger.debug(f"op_val: {op_val}")
#         #     results.append(op_val)
#         # elif isinstance(args, OpValue):
#         #     logger.debug(f"op-value with args {args}")
#         #     val = func(args.value, obj)
#         #     logger.debug(f"apply {func_name} to {args.value} -> {val}")
#         #     op_val = OpValue(value=val, derived_from=args.value, composition=[func_name] + args.composition)
#         #     logger.debug(f"op_val: {op_val}")
#         #     results.append(op_val)

#         for arg in args:

#             logger.debug(f"{func_name=} applied to {arg=}")

#             try:
#                 if isinstance(arg, PathValue):
#                     logger.debug(f"path-value with args {args}")
#                     val = func(arg.value, obj)
#                     logger.debug(f"apply {func_name} to {arg.value} -> {val}")
#                     op_val = OpValue(value=val, derived_from=arg.value, composition=[func_name])
#                     logger.debug(f"op_val: {op_val}")
#                     results.append(op_val)

#                 elif isinstance(arg, OpValue):
#                     logger.debug(f"op-value with args {args}")
#                     val = func(arg.value, obj)
#                     logger.debug(f"apply {func_name} to {arg.value} -> {val}")
#                     op_val = OpValue(value=val, derived_from=arg.value, composition=[func_name] + arg.composition)
#                     logger.debug(f"op_val: {op_val}")
#                     results.append(op_val)

#                 elif isinstance(arg, list):
#                     logger.debug("list-path-values")
#                     for a in arg:
#                         if isinstance(a, PathValue):
#                             logger.debug(f"list-path-value with arg {a}")
#                             val = func(a.value, obj)
#                             logger.debug(f"apply {func_name} to {a.value} -> {val}")
#                             op_val = OpValue(value=val, derived_from=a.value, composition=[func_name])
#                             logger.debug(f"op_val: {op_val}")
#                             results.append(op_val)
#                         elif isinstance(a, OpValue):
#                             logger.debug(f"list-op-value with arg {a}")
#                             val = func(a.value, obj)
#                             logger.debug(f"apply {func_name} to {a.value} -> {val}")
#                             op_val = OpValue(value=val, derived_from=a.value, composition=[func_name] + a.composition)
#                             logger.debug(f"op_val: {op_val}")
#                             results.append(op_val)
#                         else:
#                             logger.debug(f"new-list-path-value with arg {a}") 
#                             val = func(a, obj)
#                             logger.debug(f"apply {func_name} to {a} -> {val}")
#                             results.append(val)


#                 else:
#                     logger.debug(f"new-path-value with arg {arg}")
#                     val = func(arg, obj)
#                     logger.debug(f"apply {func_name} to {arg} -> {val}")
#                     results.append(val)

#             except Exception as e:
#                 logger.error(f"Error applying {func_name} to {arg}: {e}, skipping...")

#         logger.debug(f"results: {results}")    
#         return results
        
#     return _wrapper

# def flatten(lst, obj):
#     if not isinstance(lst, list):
#         return lst
    
#     def _helper(lst):
#         if not lst:
#             return []
#         if isinstance(lst[0], list):
#             return _helper(lst[0]) + _helper(lst[1:])
#         return lst[:1] + _helper(lst[1:])

#     return _helper(lst)
