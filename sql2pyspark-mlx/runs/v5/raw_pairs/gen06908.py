from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("k").orderBy(F.col("created_at_day").desc(), "id", "k")
w2 = Window.partitionBy("k").orderBy("created_at", "id", "k")
w3 = Window.partitionBy("k").orderBy("created_at", "id", "k").rowsBetween(Window.unboundedPreceding, Window.currentRow)

exploded = (
    df_carts
    .select(
        "id",
        "customer_id",
        "created_at",
        F.explode("attributes").alias("k", "value"),
    )
)

valid = (
    exploded
    .filter((F.col("created_at") >= F.to_date(F.lit("2025-03-02"))) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.upper("k").alias("k_upper"),
        F.coalesce("k", F.lit("NONE")).alias("k_filled"),
    )
)

filtered = (
    valid
    .filter(F.col("k").isNotNull())
    .select(
        "id",
        "created_at",
        "k",
        "value",
        "k_upper",
        "k_filled",
        F.col("created_at").cast("date").alias("created_at_day"),
        F.when(F.col("k") == "utm_source", 1).otherwise(0).alias("is_utm_source"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

gaps = (
    ranked
    .withColumn("is_new_burst", F.when(F.col("created_at") - F.lag("created_at").over(w2) > F.expr("INTERVAL 1 HOUR"), 1).otherwise(0))
)

sessionized = (
    gaps
    .select(
        "id",
        "created_at",
        "k",
        "created_at_day",
        "is_utm_source",
        "latest_rank",
        F.sum("is_new_burst").over(w3).alias("session_no"),
    )
)

result = (
    sessionized
    .groupBy("k")
    .agg(
        F.round(F.avg("is_utm_source"), 2).alias("avg_is_utm_source"),
        F.percentile_approx("session_no", 0.9).alias("session_no_p90"),
        F.avg("is_utm_source").alias("avg_is_utm_source_2"),
    )
    .orderBy(F.col("k").desc_nulls_first())
)
