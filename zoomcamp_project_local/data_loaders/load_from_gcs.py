import pyarrow as pa
import pyarrow.parquet as pq
from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'

    gcs = pa.fs.GcsFileSystem()

    bucket_name = 'mage-zoomcamp-dylan'
    table_name = 'wa_state_ev_activity_data'

    dataset_path = f"{bucket_name}/{table_name}/"  # Replace with your actual path

    parquet_data = pq.ParquetDataset(dataset_path, filesystem=gcs)

    parquet_df = parquet_data.read().to_pandas() 

    return parquet_df
