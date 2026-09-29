from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "London"]) & F.col("city").isin(["Chiang Mai", "Tokyo"]))
    .select(
        "id",
        "opened_date",
        "region",
        "sqft",
        F.length("store_name").alias("store_name_length"),
    )
)
