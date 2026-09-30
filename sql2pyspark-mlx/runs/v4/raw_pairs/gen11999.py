from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("service").orderBy(F.col("is_region").desc_nulls_last())
w2 = Window.partitionBy("attr_key")
w3 = Window.partitionBy("attr_key").orderBy(F.col("is_region").asc_nulls_first())

exploded = (
    df_app_logs
    .select("id", "service", F.explode_outer("context").alias("attr_key", "value"))
)

cleaned = (
    exploded
    .filter(F.col("attr_key").isNotNull())
    .withColumn("attr_key_filled", F.coalesce("attr_key", F.lit("Unknown")))
)

base = (
    cleaned
    .filter(F.col("attr_key").isNotNull() & F.col("attr_key").isin(["version", "region"]))
    .select(
        "*",
        F.when(F.col("attr_key") == "region", 1).otherwise(0).alias("is_region"),
    )
)

scored = (
    base
    .select(
        "*",
        F.dense_rank().over(w).alias("is_region_dense_rank"),
        F.max("is_region").over(w2).alias("attr_key_max_is_region"),
        F.rank().over(w3).alias("is_region_rank"),
        F.max("is_region").over(w2).alias("attr_key_max_is_region_2"),
    )
)

result = (
    scored
    .groupBy(
        "service",
        F.when(F.col("is_region_dense_rank") >= 7, "LARGE").otherwise("SMALL").alias("is_region_dense_rank_tier"),
        "attr_key",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.max("is_region_dense_rank").alias("max_is_region_dense_rank"),
        F.count(F.when(F.col("attr_key") == "trace", 1)).alias("trace_rows"),
        F.count_if(F.col("attr_key") == "region").alias("region_events"),
    )
    .orderBy(F.col("max_is_region_dense_rank").desc_nulls_first())
)
