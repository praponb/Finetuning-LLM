from pyspark.sql import functions as F

valid = (
    df_stores
    .filter((F.col("opened_date") >= F.lit("2024-04-16").cast("date")) & F.col("region").isNotNull())
    .select(
        "*",
        F.round("sqft", 2).alias("sqft_rounded"),
        F.quarter("opened_date").alias("opened_date_quarter"),
    )
)

result = (
    valid
    .select("region", "city")
    .subtract(
        df_stores
        .filter(~F.col("store_name").like("%Old%"))
        .select("region", "city")
    )
)
