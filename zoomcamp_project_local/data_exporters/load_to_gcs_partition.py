import pyarrow as pa
import pyarrow.parquet as pq
import os

if 'data_exporter' not in globals():
    from mage_ai.data_preparation.decorators import data_exporter

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = "/home/src/secrets/personal-gcp.json"

bucket_name = 'mage-zoomcamp-dylan'
project_id = 'galvanic-crow-412709'

table_name = "wa_state_ev_activity_data"

root_path = f"{bucket_name}/{table_name}"

gcs = pa.fs.GcsFileSystem()

@data_exporter
def export_data(data, *args, **kwargs):
    """
    Exports data to some source.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Output (optional):
        Optionally return any object and it'll be logged and
        displayed when inspecting the block run.
    """
    data['transaction_date'] = data['transaction_date'].dt.strftime("%Y-%m-%d")
    
    unique_dates = data['transaction_date'].unique()

    for transaction_date in unique_dates:
        batch_data = data[data['transaction_date'] == transaction_date]

        batch_table = pa.Table.from_pandas(batch_data)

        pq.write_to_dataset(
            batch_table,
            root_path=root_path,  # Ensure root_path is defined for each batch
            partition_cols=["transaction_date"],
            filesystem=gcs
        )
