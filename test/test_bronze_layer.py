from unittest.mock import patch
import pytest
from app.clients.ocds_client import OCDSClient
from app.services.extractors import TargetedExtractor, BulkExtractor


@pytest.fixture
def mock_client():
    with patch("app.pipelines.bronze_layer.OCDSClient") as mock_cls:
        mock_cls.return_value = OCDSClient.__new__(OCDSClient)
        yield mock_cls


def test_bronze_pipeline_init(mock_client):
    from app.pipelines.bronze_layer import BronzePipeline
    pipeline = BronzePipeline()
    assert pipeline.targeted_extractor is not None
    assert pipeline.bulk_extractor is not None
    assert pipeline.cloud_storage is None


def test_extractors_init_with_client():
    client = OCDSClient()
    targeted = TargetedExtractor(client)
    bulk = BulkExtractor(client)
    assert targeted is not None
    assert bulk is not None


def test_extractors_have_pharma_filter():
    assert TargetedExtractor.PHARMA_CATEGORIES == {"goods"}
