import asyncio
import traceback
from functools import wraps


def buffer(func):
    """
    Decorator to catch and process error messages.
    """
    if asyncio.iscoroutinefunction(func):

        @wraps(func)
        async def wrapper(*args, **kwargs):
            try:
                rslt = await func(*args, **kwargs)
            except FileExistsError:
                return None
            except FileNotFoundError:
                print(f"Could not find all files.")
                return None
            except Exception:
                traceback.print_exc()
                return None
            return rslt

    else:

        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                rslt = func(*args, **kwargs)
            except FileExistsError:
                return None
            except FileNotFoundError:
                print(f"Could not find all files.")
                return None
            except Exception:
                traceback.print_exc()
                return None
            return rslt

    return wrapper
