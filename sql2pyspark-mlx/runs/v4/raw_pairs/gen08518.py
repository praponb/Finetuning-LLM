from pyspark.sql import functions as F

lines = (
    df_carts
    .withColumn("item", F.explode_outer("items"))
    .select("id", "created_at", F.col("item.qty"))
)

result = (
    lines
    .groupBy(F.when(F.col("qty") >= 4, "LARGE").otherwise("SMALL").alias("qty_tier"))
    .agg(
        F.percentile_approx("qty", [0.25, 0.5, 0.75]).alias("qty_quartiles"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
        F.count("*").alias("event_count"),
    )
)
