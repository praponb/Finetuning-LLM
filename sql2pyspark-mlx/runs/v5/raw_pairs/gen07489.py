from pyspark.sql import functions as F

exploded = df_carts.select("id", "created_at", F.explode_outer("attributes").alias("key", "v"))

result = (
    exploded
    .groupBy("key")
    .agg(
        F.count("*").alias("event_count"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("duration_min"),
    )
    .orderBy(F.col("event_count").desc())
)
