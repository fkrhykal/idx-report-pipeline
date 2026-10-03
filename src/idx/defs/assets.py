import io
import re
import zipfile
from collections.abc import Iterable
from enum import Enum
from pathlib import Path
from typing import IO, TypedDict, cast

import dagster as dg
from ixbrlparse import IXBRL, ixbrlNonNumeric
from pydantic import BaseModel, ConfigDict

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
def financial_report_urls(
    context: dg.AssetExecutionContext, client: IdxClientResource
) -> FinancialReportURLs:
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

ixbrl_partition = dg.MultiPartitionsDefinition(
    {"year_period": year_period_partition, "emiten_code": emiten_code_partition}
)


@dg.asset(
    group_name="sources",
    partitions_def=ixbrl_partition,
    io_manager_key="zip_io_manager",
    automation_condition=dg.AutomationCondition.on_missing(),
)
def ixbrl_financial_report_zip_files(
    context: dg.AssetExecutionContext,
    financial_report_urls: dict,
    client: IdxClientResource,
) -> bytes | None:
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
            return
        res.raise_for_status()
        return res.content

    context.log.warning(
        f"[URL path Not Found] emiten_code={emiten_code} year={year} period={period} : {url_path}"
    )


def ixbrl_with_normalization(content: IO[bytes]) -> IXBRL:
    normalized = (
        content.read()
        .replace(b"ix:nonnumeric", b"ix:nonNumeric")
        .replace(b"ix:nonfraction", b"ix:nonFraction")
        .replace(b"contextref=", b"contextRef=")
        .replace(b"unitref=", b"unitRef=")
    )

    return IXBRL(io.BytesIO(normalized))


class EntryPoint(str, Enum):
    digit: str
    GENERAL = ("general", "1")
    PROPERTY = ("property", "2")
    INFRASTRUCTURE = ("infrastructure", "3")
    FINANCIAL_AND_SHARIA = ("financial_and_sharia", "4")
    SECURITIES = ("securities", "5")
    INSURANCE = ("insurance", "6")
    COLLECTIVE_INVESTMENT_CONTRACT = ("collective_investment_contract", "7")
    FINANCING = ("financing", "8")

    def __new__(cls, value: str, digit: str):
        obj = str.__new__(cls, value)
        obj._value_ = value
        obj.digit = digit
        return obj

    @classmethod
    def from_digit(cls, digit: str) -> "EntryPoint | None":
        for member in cls:
            if member.digit == digit:
                return member

    @classmethod
    def from_namelist(cls, namelist: Iterable[str]) -> "EntryPoint | None":
        for name in namelist:
            filename = Path(name).name
            if filename == "1000000.html":
                continue
            if match := re.match(r"^([1-8])\d{6}\.html$", filename):
                return cls.from_digit(match.group(1))

    def __str__(self):
        return self.value


class Metadata(BaseModel):
    model_config = ConfigDict(extra="ignore")
    EntityName: str = ""
    EntityCode: str = ""
    EntityIdentificationNumber: str = ""
    EntityMainIndustry: str = ""
    Sector: str = ""
    Subsector: str = ""
    TypeOfEntity: str = ""
    TypeOfListedSecurities: str = ""
    EntryPoint: EntryPoint | None


def parse_ixbrl_financial_report_metadata(zip_ixbrl: bytes) -> Metadata | None:
    with zipfile.ZipFile(io.BytesIO(zip_ixbrl)) as z:
        dei_file = "1000000.html"
        namelist = z.namelist()
        if dei_file not in namelist:
            return
        with z.open(dei_file) as f:
            dei = ixbrl_with_normalization(f)
            nonnumerics: list[ixbrlNonNumeric] = dei.nonnumeric
            model = {item.name: item.value for item in nonnumerics}
            model["EntryPoint"] = EntryPoint.from_namelist(namelist)
            return Metadata.model_validate(model)


@dg.asset(
    group_name="stagings",
    partitions_def=ixbrl_partition,
    io_manager_key="json_io_manager",
    automation_condition=dg.AutomationCondition.on_missing(),
)
def ixbrl_financial_report_metadata(ixbrl_financial_report_zip_files: bytes | None):
    if ixbrl_financial_report_zip_files is None:
        return
    metadata = parse_ixbrl_financial_report_metadata(ixbrl_financial_report_zip_files)
    if metadata is not None:
        return metadata.model_dump()
