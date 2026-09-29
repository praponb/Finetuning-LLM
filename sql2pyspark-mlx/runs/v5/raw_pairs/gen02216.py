from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isin(["New York", "Berlin"]))
    .select(
        "supplier_name",
        F.concat_ws("-", "city", "country").alias("city_country_key"),
    )
)
