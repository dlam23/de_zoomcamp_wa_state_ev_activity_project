import io
import pandas as pd
import requests
from datetime import date, datetime, timedelta
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """
    execution_date_obj = kwargs['execution_date'].date()

    days_to_subtract = 366 if date.today().year % 4 == 0 else 365

    # Subtract 1 year from the execution date
    start_date_obj = execution_date_obj - timedelta(days=days_to_subtract) - timedelta(days=1)

    # Format the start date as YYYY-MM-DDT00:00:00.000
    start_date = start_date_obj.strftime("%Y-%m-%dT00:00:00.000")

    url = 'https://data.wa.gov/resource/rpr4-cgyd.csv?$limit=2000&transaction_date=' + start_date

    parse_dates = ['date_of_vehicle_sale', 'transaction_date']

    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep=',', parse_dates=parse_dates)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
