import dagster as dg
from curl_cffi import Response, Session


class IdxScraperResource(dg.ConfigurableResource):
    def get(self, url: str, **kwargs) -> Response:
        session = Session(impersonate="chrome124")
        session.headers.update(
            {
                "Referer": "https://www.idx.co.id/",
            }
        )
        response: Response = session.get(url, **kwargs)
        return response


@dg.definitions
def resources():
    return dg.Definitions(
        resources={
            "client": IdxScraperResource(),
        }
    )
