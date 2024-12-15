import json
import importlib
import datetime
import math
import re
import functools
import statistics
import numpy as np

def load_builtins(json_path):
    with open(json_path, "r") as f:
        funcs = json.load(f)
    
    builtins = {}
    
    for name, typ, nargs, code, _ in funcs.items():
        builtsin['name'] = load_builtin(name, typ, nargs, code)

    return builtins

def load_builtin(name, typ, nargs, code):
    if typ == "lambda":
        try:
            entry = (eval(code), nargs)
        except Exception as e:
            raise ValueError(f"Error loading lambda '{name}': {e}")
    
    elif typ == "function":
        try:
            if isinstance(code, list):
                code = "\n".join(code)
            elif not isinstance(code, str):
                raise TypeError(f"Function '{name}' code must be a string or list of strings.")
            
            local_scope = {}
            exec(code, globals(), local_scope)
            f = local_scope.get(name)
            if func is None:
                raise ValueError(f"Function '{name}' not found after executing code.")

            return (f, nargs)
            
        except Exception as e:
            raise ValueError(f"Error loading function '{name}': {e}")
    
    else:
        raise ValueError(f"Unknown function type '{typ}' for '{name}'.")
