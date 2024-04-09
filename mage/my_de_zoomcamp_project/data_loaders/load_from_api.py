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

    schema = {
            'electric_vehicle_type': str,
            'vin_1_10': str,
            'dol_vehicle_id': str,
            'model_year': pd.Int64Dtype(),
            'make': str,
            'model': str,
            'vehicle_primary_use': str,
            'electric_range': pd.Int64Dtype(),
            'odometer_reading': pd.Int64Dtype(),
            'odometer_code': str,
            'new_or_used_vehicle': str,
            'sale_price': float,
            'base_msrp': float,
            'transaction_type': str,
            'transaction_year': pd.Int64Dtype(),
            'county': str,
            'city': str,
            'state_of_residence': str,
            'zip': str,
            'meets_2019_hb_2042_sale_price_value_requirement': bool,
            '_2019_hb_2042_sale_price_value_requirement': str,
            'electric_vehicle_fee_paid': str,
            'transportation_electrification_fee_paid': str,
            'hybrid_vehicle_electrification_fee_paid': str,
            'census_tract_2020': str,
            'legislative_district': str,
            'electric_utility': str
    }

    parse_dates = ['transaction_date']

    response = requests.get(url)
    df = pd.read_csv(io.StringIO(response.text), sep=',', dtype=schema, parse_dates=parse_dates)

    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
