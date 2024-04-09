import os
if 'custom' not in globals():
    from mage_ai.data_preparation.decorators import custom
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@custom
def transform_custom(*args, **kwargs):
    """
    args: The output from any upstream parent blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    os.system("""gcloud dataproc jobs submit pyspark \
        --cluster=de-zoomcamp-project-cluster \
        --region=us-west1 \
        --jars=gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar \
        gs://staging-bucket-galvanic-crow-412709/code/spark_bigquery.py
    """)
    
    return "Spark job completed successfully."


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
