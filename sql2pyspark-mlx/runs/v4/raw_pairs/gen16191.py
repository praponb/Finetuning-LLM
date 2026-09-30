from pyspark.sql import functions as F

base = (
    df_stores
    .filter(F.col("sqft") < 8252)
    .select(
        "*",
        F.when(F.col("sqft") >= 16317, "High").when(F.col("sqft") >= 7029, "Medium").otherwise("Low").alias("sqft_level"),
        F.trim("store_name").alias("clean_store_name"),
        F.coalesce("region", F.lit("DIRECT")).alias("region_clean"),
    )
)

result = (
    base
    .groupBy("city")
    .pivot("region", ["East", "Central"])
    .agg(F.avg("sqft").alias("value_sum"))
    .select("city", F.col("East").alias("east"), F.col("Central").alias("central"))
)
