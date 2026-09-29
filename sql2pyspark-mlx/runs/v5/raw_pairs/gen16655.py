from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-1800, Window.currentRow)
w2 = Window.partitionBy("k").orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)
w3 = Window.orderBy(F.col("days_since_created_at_rounded").desc_nulls_last())
w_ordered = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-900, Window.currentRow)
w_part = Window.partitionBy("k")

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "value"),
    )
)

filtered = (
    lines
    .filter(F.col("k").isNotNull())
    .select(
        "*",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
    )
)

valid = (
    filtered
    .filter((F.col("created_at") > F.lit("2025-03-02 00:00:00").cast("timestamp")) & (~F.col("k").eqNullSafe(F.lit("gift"))))
    .select(
        "*",
        F.round("days_since_created_at", 2).alias("days_since_created_at_rounded"),
    )
)

base = (
    valid
    .filter((F.col("created_at") >= F.to_timestamp(F.lit("2025-03-01 23:00:00"))) & (~F.col("k").eqNullSafe(F.lit("device"))))
    .withColumn("days_since_created_at_rounded_plus_days_since_created_at", (F.col("days_since_created_at_rounded") + F.col("days_since_created_at")))
    .withColumn("least_days_since_created_at_days_since_created_at_rounded", F.least("days_since_created_at", "days_since_created_at_rounded"))
)

base_carts = (
    base
    .filter(F.col("least_days_since_created_at_days_since_created_at_rounded").between(61, 80))
    .withColumn("least_days_since_created_at_days_since_created_at_rounded_filled", F.coalesce("least_days_since_created_at_days_since_created_at_rounded", F.lit(0)))
    .withColumn("least_days_since_created_at_days_since_created_at_rounded_filled_2", F.coalesce("least_days_since_created_at_days_since_created_at_rounded", F.lit(0)))
)

result = (
    base_carts
    .select(
        "created_at",
        "coupon_codes",
        "days_since_created_at",
        "days_since_created_at_rounded_plus_days_since_created_at",
        "least_days_since_created_at_days_since_created_at_rounded",
        "least_days_since_created_at_days_since_created_at_rounded_filled",
        F.sum("least_days_since_created_at_days_since_created_at_rounded_filled_2").over(w_grp).alias("least_days_since_created_at_days_since_created_at_rounded_filled_2_sum_30m"),
        F.sum("days_since_created_at").over(w2).alias("days_since_created_at_sum_1h"),
        F.cume_dist().over(w3).alias("days_since_created_at_rounded_cume_dist"),
        F.sum("days_since_created_at").over(w_ordered).alias("days_since_created_at_sum_15m"),
        F.count("least_days_since_created_at_days_since_created_at_rounded").over(w_part).alias("k_count_least_days_since_created_at_days_since_created_at_rounded"),
    )
)
