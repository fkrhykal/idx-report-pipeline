import json
from pathlib import Path

import dagster as dg

from idx.defs.resources import IdxScraperResource

# class Emiten(BaseModel):
#     ticker: str = Field(alias="KodeEmiten", min_length=4, max_length=6)
#     company_name: str = Field(alias="NamaEmiten")


#     @staticmethod
#     def adapter():


#     @staticmethod
#     def list_from_json(obj: object):
#         return EmitenListAdapter.


# EmitenListAdapter = TypeAdapter(list[Emiten])


@dg.asset(group_name="sources")
def source_emitens(client: IdxScraperResource):
    res = client.get("https://www.idx.co.id/primary/Helper/GetEmiten?emitenType=*")
    raw_data = res.json()

    file_path = Path("data/sources/emitens.json")
    file_path.parent.mkdir(parents=True, exist_ok=True)

    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(raw_data, f, ensure_ascii=False)

    return dg.Output(
        value=file_path,
        metadata={
            "file_path": dg.MetadataValue.path(str(file_path.resolve())),
            "total_records": len(raw_data),
        },
    )
