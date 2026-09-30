from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "product_id",
        F.when(F.col("units") >= 39, "PREMIUM").otherwise("STANDARD").alias("units_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.bit_or("units").alias("units_bit_or"),
        F.percentile_approx("units", [0.25, 0.5, 0.75]).alias("units_quartiles"),
    )
    .filter(F.col("event_count") < 1)
    .orderBy(F.col("units_tier").asc_nulls_last())
)
