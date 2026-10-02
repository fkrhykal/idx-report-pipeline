from typing import TypedDict, cast

import dagster as dg

from idx.defs.resources import IdxClientResource

# @dg.asset(
#     group_name="sources",
#     automation_condition=dg.AutomationCondition.on_missing(),
#     io_manager_key="json_io_manager",
# )
# def emitens(client: IdxClientResource):
#     res = client.get("https://www.idx.co.id/primary/Helper/GetEmiten?emitenType=*")
#     res.raise_for_status()
#     return res.json()


# @dg.asset(
#     group_name="sources",
#     automation_condition=dg.AutomationCondition.on_missing(),
#     io_manager_key="json_io_manager",
# )
# def company_profies(client: IdxClientResource):
#     res = client.get(
#         "https://www.idx.co.id/primary/ListedCompany/GetCompanyProfiles?emitenType=s&start=0&length=9999&lang=en"
#     )
#     res.raise_for_status()
#     return res.json()


class Attachment(TypedDict):
    Emiten_Code: str
    File_ID: str
    File_Modified: str
    File_Name: str
    File_Path: str
    File_Size: int
    File_Type: str
    Report_Period: str
    Report_Type: str
    Report_Year: str
    NamaEmiten: str


class Result(TypedDict):
    KodeEmiten: str
    Attachments: list[Attachment]


class FinancialReportURLs(TypedDict):
    Search: dict
    ResultCount: int
    Results: list[Result]


YEARS = ["2021", "2022", "2023", "2024", "2025", "2026"]
PERIODS = ["TW1", "TW2", "TW3", "audit"]

year_period_partition = dg.StaticPartitionsDefinition(
    [f"{year}_{period}" for year in YEARS for period in PERIODS]
)


@dg.asset(
    group_name="sources",
    partitions_def=year_period_partition,
    automation_condition=dg.AutomationCondition.on_missing(),
    io_manager_key="json_io_manager",
)
def financial_report_urls(context: dg.AssetExecutionContext, client: IdxClientResource):
    partition: str = context.partition_key
    year, period = partition.split("_")

    url = f"https://www.idx.co.id/primary/ListedCompany/GetFinancialReport?periode={period}&year={year}&indexFrom=0&pageSize=1000&reportType=rdf"
    res = client.get(url)
    res.raise_for_status()
    data: FinancialReportURLs = res.json()

    emiten_codes = [result["KodeEmiten"] for result in data["Results"]]

    cast(dg.DagsterInstance, context.instance).add_dynamic_partitions(
        partitions_def_name="emiten_code", partition_keys=emiten_codes
    )

    return data


emiten_code_partition = dg.DynamicPartitionsDefinition(name="emiten_code")


@dg.asset(
    group_name="sources",
    deps=[financial_report_urls],
    partitions_def=dg.MultiPartitionsDefinition(
        {"year_period": year_period_partition, "emiten_code": emiten_code_partition}
    ),
    io_manager_key="zip_io_manager",
)
def ixbrl_financial_report_zip_files(
    context: dg.AssetExecutionContext,
    financial_report_urls: dict,
    client: IdxClientResource,
):
    data = cast(FinancialReportURLs, financial_report_urls)
    partition: dg.MultiPartitionKey = context.multi_partition_key
    emiten_code = partition.keys_by_dimension["emiten_code"]
    year, period = partition.keys_by_dimension["year_period"].split("_")

    url_path: None | str = None

    for result in data["Results"]:
        if result["KodeEmiten"] != emiten_code:
            continue
        for attacthment in result["Attachments"]:
            if attacthment["File_Name"] != "inlineXBRL.zip":
                continue
            url_path = f"https://www.idx.co.id/{attacthment['File_Path']}"

    if url_path is not None:
        res = client.get(url_path)
        if res.status_code == 404:
            context.log.warning(
                f"[File zip 404 Not Found] emiten_code={emiten_code} year={year} period={period} : {url_path}"
            )
            return None
        res.raise_for_status()
        return res.content

    context.log.warning(
        f"[URL path Not Found] emiten_code={emiten_code} year={year} period={period} : {url_path}"
    )
    return None
