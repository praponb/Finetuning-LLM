from pyspark.sql import functions as F

exploded = (
    df_carts
    .select("id", "created_at", F.explode_outer("attributes").alias("k", "value"))
)

result = (
    exploded
    .groupBy(F.date_trunc("HOUR", "created_at").alias("hour_window"))
    .agg(
        F.count("*").alias("cnt"),
        ((F.unix_timestamp(F.max("created_at")) - F.unix_timestamp(F.min("created_at"))) / 60).alias("span_minutes"),
    )
    .orderBy(F.col("cnt").desc_nulls_last())
)
