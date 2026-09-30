from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("days_since_created_at").desc_nulls_first())
w2 = Window.orderBy("days_since_created_at")
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-7200, Window.currentRow)
w4 = Window.orderBy(F.col("days_since_created_at").desc())

flattened = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode("attributes").alias("key", "value"),
    )
)

lines = (
    flattened
    .select(
        "id",
        "created_at",
        "key",
        "value",
        F.explode("coupon_codes").alias("value_2"),
    )
)

valid = (
    lines
    .filter(F.col("created_at") > F.lit("2025-03-01").cast("date"))
    .select(
        "id",
        "created_at",
        "value",
        F.when(F.col("value_2").isin(["SPRING10", "BUNDLE"]), "Tier 1").otherwise("Other").alias("value_2_group"),
    )
)

cleaned = (
    valid
    .filter(F.hour("created_at").between(8, 12))
    .withColumn("days_since_created_at", F.datediff(F.current_date(), "created_at"))
)

cleaned_carts = (
    cleaned
    .filter(F.col("created_at") >= F.lit("2025-03-01").cast("date"))
    .select("*", F.col("created_at").cast("date").alias("created_at_day"))
)

with_windows = (
    cleaned_carts
    .withColumn("days_since_created_at_dense_rank", F.dense_rank().over(w))
    .withColumn("days_since_created_at_rank", F.rank().over(w2))
    .withColumn("days_since_created_at_avg_2h", F.avg("days_since_created_at").over(w3))
    .withColumn("days_since_created_at_percent_rank", F.percent_rank().over(w4))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("days_since_created_at") >= 75, "PREMIUM").otherwise("STANDARD").alias("days_since_created_at_tier"),
    )
    .agg(
        F.min("created_at").alias("started_at"),
        F.max("created_at").alias("ended_at"),
        F.percentile_approx("days_since_created_at", [0.25, 0.5, 0.75]).alias("days_since_created_at_quartiles"),
        F.sum("days_since_created_at_rank").alias("total_days_since_created_at_rank"),
        F.min("days_since_created_at").alias("min_days_since_created_at"),
    )
    .orderBy(F.col("days_since_created_at_tier").desc_nulls_first())
)
