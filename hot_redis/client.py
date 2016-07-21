
import contextlib
import os
import threading

import redis


class HotClient(redis.Redis):
    """
    A Redis client wrapper that loads Lua functions and creates
    client methods for calling them.
    """

    def __init__(self, *args, **kwargs):
        kwargs.setdefault("decode_responses", True)
        super(HotClient, self).__init__(*args, **kwargs)
        requires_luabit = ("number_and", "number_or", "number_xor",
                           "number_lshift", "number_rshift")
        with open(self._get_lua_path("bit.lua")) as f:
            luabit = f.read()
        for name, snippet in self._get_lua_funcs():
            if name in requires_luabit:
                snippet = luabit + snippet
            self._create_lua_method(name, snippet)

    def _get_lua_path(self, name):
        """
        Joins the given name with the relative path of the module.
        """
        parts = (os.path.dirname(os.path.abspath(__file__)), "lua", name)
        return os.path.join(*parts)

    def _get_lua_funcs(self):
        """
        Returns the name / code snippet pair for each Lua function
        in the atoms.lua file.
        """
        with open(self._get_lua_path("atoms.lua")) as f:
            for func in f.read().strip().split("function "):
                if func:
                    bits = func.split("\n", 1)
                    name = bits[0].split("(")[0].strip()
                    snippet = bits[1].rsplit("end", 1)[0].strip()
                    yield name, snippet

    def _create_lua_method(self, name, code):
        """
        Registers the code snippet as a Lua script, and binds the
        script to the client as a method that can be called with
        the same signature as regular client methods, eg with a
        single key arg.
        """
        script = self.register_script(code)
        setattr(script, "name", name)  # Helps debugging redis lib.
        method = lambda key, *a, **k: script(keys=[key], args=a, **k)
        setattr(self, name, method)


_thread = threading.local()
_config = {}


def default_client():
    try:
        _thread.client
    except AttributeError:
        setattr(_thread, "client", HotClient(**_config))
    return _thread.client


def configure(**config):
    global _config
    _config = config


@contextlib.contextmanager
def transaction():
    """
    Swaps out the current client with a pipeline instance,
    so that each Redis method call inside the context will be
    pipelined. Once the context is exited, we execute the pipeline.
    """
    client = default_client()
    _thread.client = client.pipeline()
    try:
        yield
        _thread.client.execute()
    finally:
        _thread.client = client


