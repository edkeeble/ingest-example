import pathlib
from typing import Sequence
from ingest.app import IngestApp
from ingest.data_types import S3Object
from ingest.pipeline import Pipeline
from ingest.step import Transformer, Collector
from ingest.trigger import S3Filter, S3ObjectCreated
from csda_ingest.data_models import SpireItem, StacItem, Nothing


class ExtractSpire(Transformer[S3Object, SpireItem]):
    @classmethod
    def execute(self, input: S3Object) -> SpireItem:
        return SpireItem(id=1, name=input.bucket, location=input.key)


class SpireToStac(Transformer[SpireItem, StacItem]):
    @classmethod
    def execute(self, input: SpireItem) -> StacItem:
        return StacItem(
            id=input.id,
            properties={
                "name": input.name,
                "location": input.location,
            },
        )


class LoadToPgstac(Collector[StacItem, StacItem]):
    batch_size: int = 3

    @classmethod
    def execute(self, input: Sequence[StacItem]) -> StacItem:
        for item in input:
            print(f"Loading {item}")
        return input[0]


spire_pipeline = Pipeline(
    "Spire Ingest",
    trigger=S3ObjectCreated(
        bucket_name="ekeeble-ingest-test",
        object_filter=S3Filter(prefix="inbox", suffix=".json"),
    ),
    steps=[ExtractSpire, SpireToStac, LoadToPgstac],
)

csda_app = IngestApp(
    "CSDA Ingest",
    code_dir=str(pathlib.Path(__file__).parent.resolve()),
    requirements_path="requirements.txt",
    pipelines=[spire_pipeline],
)
