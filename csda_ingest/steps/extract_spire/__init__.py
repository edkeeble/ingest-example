from pathlib import Path
from ingest.step import Transformer
from ingest.permissions import S3ReadAccess
from ingest.data_types import S3Object
from csda_ingest.data_models import SpireItem


class ExtractSpire(Transformer[S3Object, SpireItem]):
    permissions = [S3ReadAccess(bucket_name="ekeeble-ingest-test")]
    requirements_path = Path(Path(__file__).parent.resolve() / "requirements.txt")

    @classmethod
    def execute(self, input: S3Object) -> SpireItem:
        import json
        from smart_open import smart_open

        with smart_open(f"s3://{input.bucket}/{input.key}") as f:
            data = json.load(f)
            item = SpireItem(**data)
            return item
