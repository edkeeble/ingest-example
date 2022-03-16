import pathlib
from typing import Sequence
from ingest.app import IngestApp
from ingest.pipeline import Pipeline
from ingest.step import Transformer, Collector
from csda_ingest.data_models import S3Item, SpireItem, StacItem, Nothing


class ExtractSpire(Transformer[S3Item, SpireItem]):
    @classmethod
    def execute(self, input: S3Item) -> SpireItem:
        return SpireItem(id=1, name="test", location=input.key)


class DoNothing(Transformer[StacItem, Nothing]):
    @classmethod
    def execute(self, input: StacItem) -> Nothing:
        return Nothing()


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


spire_pipeline = Pipeline("Spire Ingest", steps=[ExtractSpire, SpireToStac, LoadToPgstac])

csda_app = IngestApp(
    "CSDA Ingest",
    code_dir=str(pathlib.Path(__file__).parent.resolve()),
    requirements_path="requirements.txt",
    pipelines=[spire_pipeline],
)
