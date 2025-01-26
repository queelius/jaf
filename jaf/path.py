from typing import Any, List, Union
import re

class Value:
    def __init__(self, value: Any, path: List[str] = None):
        self.value = value
        self.path = path or []

    def __repr__(self):
        return f"{self.__class__.__name__}(value={self.value}, path={self.path})"

class PathValue(Value):
    pass

class OpValue(Value):
    def __init__(self, value: Any, derived_from: List[Any] = None, composition: List[str] = None):
        super().__init__(value, [])
        self.derived_from = derived_from or []
        self.composition = composition or []

    def __repr__(self):
        return (f"{self.__class__.__name__}(value={self.value}, "
                f"derived_from={self.derived_from}, composition={self.composition})")

def path_values(path: Union[str, List[str]], obj: Any) -> List[PathValue]:
    if isinstance(path, str):
        path = re.findall(r'\*\*|\*|\[[^\]]+\]|[^.]+', path.strip())

    if not isinstance(path, list):
        raise ValueError("Invalid path type. Must be str or list.")
    
    if path[0] == '$':
        path = path[1:]

    return path_values_ast(obj, path)

def path_values_ast(obj: Any, path: List[str]) -> List[PathValue]:

    def _flatten(paths: List[PathValue]) -> List[PathValue]:
        results = []
        for p in paths:
            if isinstance(p.value, list):
                # create a PathValue for each element in the list
                for item in p.value:
                    results.append(PathValue(item, p.path))
            else:
                results.append(p)

        return results


    def _get_path(o: Any, p: List[str], ps: List[str]) -> List[PathValue]:
        if len(p) == 0:
            return [PathValue(o, ps)]

        token = p[0]
        results = []

        if token == '**':
            results.extend(_get_path(o, p[1:], ps))

            if isinstance(o, dict):
                for k, v in o.items():
                    results.extend(_get_path(v, p, ps + [k]))
            elif isinstance(o, list):
                for i, item in enumerate(o):
                    results.extend(_get_path(item, p, ps + [f"[{i}]"]))
        elif token == '*':
            if isinstance(o, dict):
                for k, v in o.items():
                    results.extend(_get_path(v, p[1:], ps + [k]))
            elif isinstance(o, list):
                for i, item in enumerate(o):
                    results.extend(_get_path(item, p[1:], ps + [f"[{i}]"]))
        elif re.match(r'\[\d+\]', token):
            idx = int(token.strip('[]'))
            if isinstance(o, list) and 0 <= idx < len(o):
                results.extend(_get_path(o[idx], p[1:], ps + [f"[{idx}]"]))
        else:
            if isinstance(o, dict) and token in o:
                results.extend(_get_path(o[token], p[1:], ps + [token]))

        return results

    paths = _get_path(obj, path, ['$'])
    #return _flatten(paths)
    return paths