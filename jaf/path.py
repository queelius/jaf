from typing import Any, List, Union
import re

class PathValue:
    """
    A class to represent a value from a path in a JSON object.

    Args:
        value (Any): The value of the object at the path.
        path (List[str], optional): The path in the object. Defaults to None.
    """
    def __init__(self, value: Any, path: List[str] = None):
        self.value = value
        self.path = path

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value}, path={self.path})"

    #def __str__(self):
    #    return str(self.value)

class OpValue:
    def __init__(self,
                 value: Any,
                 derived_from: PathValue = None,
                 composition: List[str] = None):
        """
        Most of the time, `value` will be a primitive type (int, float, str, bool).
        It is usually derived from an operation on the path in the object. The
        `derived_from` attribute is a `PathValue` object that represents the
        path in the object from which the value was a composition of operations,

            value = op_n(...,op2(op1(derived_from)))

        where `composition = ['op_n', ..., 'op2', 'op1']` is the list of
        operations that were applied to the `dervied_from` path value.
        
        This is useful for debugging and understanding how the value was derived.

        Args:
            value (Any): The value of the object.
            derived_from (PathValue, optional): The path value from which the value was derived. Defaults to None.
            composition (List[str], optional): The list of operations applied to the `derived_from` value. Defaults to None.
        """
                
        self.value = value
        self.derived_from = derived_from
        self.composition = composition

    def apply(self, op: str, value: Any):
        """
        Apply an operation to the value and return a new `Value` object.
        """
        return OpValue(value=self.value,
                       derived_from=self.derived_from,
                       composition=[op] + self.composition)

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value}, derived_from={self.derived_from}, composition={self.composition})"

def path_values(path: Union[str, List[str]], obj: Any) -> List[PathValue]:

    if isinstance(path, str):
        path = re.findall(r'\*\*|\*|\[[^\]]+\]|[^.]+', path.strip())

    if not isinstance(path, list):
        raise ValueError("Invalid path type. Must be str or list.")
    
    if path[0] == '$':
        path = path[1:]

    return path_values_ast(obj, path)

def path_values_ast(obj: Any,
                    path: List[str]) -> List[PathValue]:

    def _get_path(o: Any, p: List[str], ps: List):

        if len(p) == 0:
            return [PathValue(o, ps)]

        token = p[0]
        results = []

        if token == '**':
            results.extend(_get_path(o, p[1:], ps))

            if isinstance(o, dict):
                for k, v in o.items():
                    results.extend(_get_path(v, p, ps+[k]))
            elif isinstance(o, list):
                for i, item in enumerate(o):
                    results.extend(_get_path(item, p, ps+[[i]]))
        elif token == '*':
            if isinstance(o, dict):
                for k, v in o.items():
                    results.extend(_get_path(v, p[1:], ps+[k]))
            elif isinstance(o, list):
                for i, item in enumerate(o):
                    results.extend(_get_path(item, p[1:], ps+[[i]]))
        elif re.match(r'\[\d+\]', token):
            idx = int(token.strip('[]'))
            if isinstance(o, list) and 0 <= idx < len(o):
                results.extend(_get_path(o[idx], p[1:], ps+[[idx]]))
        elif isinstance(o, dict) and token in o:
            results.extend(_get_path(o[token], p[1:], ps+[token]))

        return results

    return _get_path(o =obj, p=path, ps=['$'])


def has_path(path, obj):
    """
    Checks if `obj` satisfies the path query.

    For example, suppose we the following object:

    ```python
    obj = {
        'name': 'John Doe',
        'age': 30,
        'address': {
            'city': 'New York',
            'zip': 10001
        }
        'alternate_address': {
            'city': 'Some City',
            'zip': 'No Zip'
        }
    }
    ```

    The following path query will return `True`:
    
    ```python
    path = "*.city" # or ['*', 'city']
    has_path(path, obj) # True
    ```
    """
    path_vals = path_values(path, obj)
    return len(path_vals) > 0

def has_path_value_type(path, obj, type):
    """
    Checks if the path exists in the object and the value at that path is of the specified type.

    Consider the object example in `has_path` function. The following query will return `True`:

    ```python
    path = "*.zip"
    has_path_value_type(path, obj, int) # True because the value at path 'adress.zip' is an integer.
    ```
    """
    path_vals = path_values(path, obj)
    return any([isinstance(pv.value, type) for pv in path_vals])

def has_path_value(path, obj, value):
    """
    Checks if the path exists in the object and the value at that path is equal to the specified value.

    Consider the object example in `has_path` function. The following query will return `True`:
    
    ```python
    path = "*.city"
    has_path_value(path, obj, 'Some City') # True because the value at path 'alternate_address.city' is 'Some City'.
    """
    path_vals = path_values(path, obj)
    return any([pv.value == value for pv in path_vals])


def has_path_components(path, obj, components):
    """
    Checks if the path exists in the object and the path in the object contains all the specified components.

    Consider the object example in `has_path` function. The following query will return `True`:
    
    ```python
    path = "*.city"
    path_has_key(path, obj, ['address']) # True because the object has a path 'address.city' and
                                         # 'address' is a component of the path.
    """
    path_vals = path_values(path, obj)
    return any([all([c in pv.value for c in components]) for pv in path_vals])
