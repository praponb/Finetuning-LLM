from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") != "UK"))
    .select(
        "*",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
    )
)

result = (
    filtered
    .groupBy("city")
    .agg(
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.stddev_samp("rating").alias("rating_stddev"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.sum(F.when(F.col("country") == "JP", F.col("rating")).otherwise(0)).alias("jp_rating"),
    )
    .orderBy(F.col("jp_rating").desc_nulls_first())
)
