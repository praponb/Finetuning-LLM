from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.year("opened_date") >= 2023))
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

result = (
    base
    .withColumn("sqft_tier", F.when(F.col("sqft") >= 17297, "PREMIUM").otherwise("STANDARD"))
    .groupingSets([["region", "sqft_tier"], ["region"], []], "region", "sqft_tier")
    .agg(
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "North", 1)), F.lit(0)), 2).alias("sqft_per_north"),
        F.count(F.when(F.col("region") == "West", 1)).alias("west_rows"),
    )
)
