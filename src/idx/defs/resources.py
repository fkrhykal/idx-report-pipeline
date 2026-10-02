import json
from typing import Any

import dagster as dg
from curl_cffi import Response, Session
from upath import UPath


class IdxClientResource(dg.ConfigurableResource):
    def get(self, url: str, **kwargs) -> Response:
        session = Session(impersonate="chrome124")
        session.headers.update(
            {
                "Referer": "https://www.idx.co.id/",
            }
        )
        response: Response = session.get(url, **kwargs)
        return response


class JsonIOManager(dg.UPathIOManager):
    extension = ".json"

    def dump_to_path(self, context: dg.OutputContext, obj: Any, path: UPath):
        with path.open("w", encoding="utf-8") as fp:
            json.dump(obj, fp)

    def load_from_path(self, context: dg.InputContext, path: UPath) -> Any:
        with path.open("r", encoding="utf-8") as fp:
            return json.load(fp)


class ZipIOManager(dg.UPathIOManager):
    extension = ".zip"

    def dump_to_path(self, context: dg.OutputContext, obj: bytes, path: UPath):
        with path.open("wb") as f:
            f.write(obj)

    def load_from_path(self, context: dg.InputContext, path: UPath) -> Any:
        with path.open("rb") as f:
            return f.read()


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "client": IdxClientResource(),
            "json_io_manager": JsonIOManager(base_path=UPath("data")),
            "zip_io_manager": ZipIOManager(base_path=UPath("data")),
        }
    )
