from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("attr_key").orderBy(F.col("created_at").desc(), "id", "attr_key")
w2 = Window.partitionBy("attr_key").orderBy("created_at", "id", "attr_key")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("attr_key", "value"),
    )
)

scored = (
    lines
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("event_seq_2", F.row_number().over(w2))
)

valid = (
    scored
    .filter(F.col("attr_key").isNotNull() & (F.col("event_seq_2") < 6))
    .withColumn("is_welcome5", F.array_contains("coupon_codes", "WELCOME5"))
)

result = (
    valid
    .groupBy("attr_key")
    .agg(
        F.percentile_approx("event_seq_2", 0.9).alias("event_seq_2_p90"),
        F.percentile_approx("event_seq", [0.25, 0.5, 0.75]).alias("event_seq_quartiles"),
        F.min("created_at").alias("started_at"),
        F.min("event_seq_2").alias("min_event_seq_2"),
        F.count("*").alias("cnt"),
    )
)
