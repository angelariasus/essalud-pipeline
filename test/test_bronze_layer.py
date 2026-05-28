import pytest
from app.pipelines.bronze_layer import BronzePipeline
from app.services.extractors import TargetedExtractor, BulkExtractor

def test_bronze_pipeline_init():
    # Verifica que el pipeline se pueda inicializar correctamente
    pipeline = BronzePipeline()
    assert pipeline.extractor is not None
    assert pipeline.r2_manager is not None

def test_extractors_init():
    targeted = TargetedExtractor()
    bulk = BulkExtractor()
    assert targeted is not None
    assert bulk is not None
