from pyspark.sql.functions import col, to_date
if 'transformer' not in globals():
    from mage_ai.data_preparation.decorators import transformer
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@transformer
def transform(data, *args, **kwargs):
    """
    Template code for a transformer block.

    Add more parameters to this function if this block has multiple parent blocks.
    There should be one parameter for each output variable from each parent block.

    Args:
        data: The output from the upstream parent block
        args: The output from any additional upstream blocks (if applicable)

    Returns:
        Anything (e.g. data frame, dictionary, array, int, str, etc.)
    """
    # Specify your transformation logic here

    spark = kwargs.get('spark')
    df = spark.createDataFrame(data)
    df = df.drop(
        "vin_1_10",
        "electric_range",
        "odometer_reading",
        "odometer_code",
        "sale_price",
        "date_of_vehicle_sale",
        "base_msrp",
        "meets_2019_hb_2042_sale_price_value_requirement",
        "_2019_hb_2042_sale_price_value_requirement",
        "electric_vehicle_fee_paid",
        "transportation_electrification_fee_paid",
        "hybrid_vehicle_electrification_fee_paid",
        "census_tract_2020",
        "legislative_district",
        "electric_utility"
    )

    df.createOrReplaceTempView("ev")

    df_result = spark.sql("""
        SELECT transaction_date,
        transaction_year,
        transaction_type,
        electric_vehicle_type,
        model_year,
        make,
        model,
        vehicle_primary_use,
        new_or_used_vehicle,
        county,
        city,
        state_of_residence AS state,
        zip,
        COUNT(DISTINCT dol_vehicle_id) AS vehicle_registration_count
        FROM ev
        GROUP BY transaction_date,
        transaction_year,
        transaction_type,
        electric_vehicle_type,
        model_year,
        make,
        model,
        vehicle_primary_use,
        new_or_used_vehicle,
        county,
        city,
        state_of_residence,
        zip
    """)

    final_result = df_result.toPandas()
    spark.stop()

    return final_result


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'