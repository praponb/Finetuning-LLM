from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("value").orderBy(F.col("days_since_created_at").asc_nulls_first())
w2 = Window.partitionBy("value").orderBy(F.col("days_since_created_at").desc_nulls_last())
w3 = Window.orderBy(F.col("created_at").cast("bigint")).rangeBetween(-3600, Window.currentRow)

flattened = df_carts.select("id", "created_at", F.explode("coupon_codes").alias("value"))

kept_keys = (
    flattened
    .select("id")
    .intersectAll(
        df_carts
        .filter(F.array_contains("coupon_codes", "VIP"))
        .select("id")
    )
)

kept_rows = (
    flattened
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.hour("created_at").between(7, 12))
    .select(
        "*",
        F.quarter("created_at").alias("created_at_quarter"),
        F.coalesce("value", F.lit("OTHER")).alias("resolved_value"),
    )
)

valid = (
    cleaned
    .filter(F.col("created_at") > F.lit("2025-03-01 18:00:00").cast("timestamp"))
    .select(
        "id",
        "created_at",
        "value",
        "resolved_value",
        F.datediff(F.current_date(), "created_at").alias("days_since_created_at"),
        F.length("value").alias("value_length"),
        F.coalesce("value", F.lit("DIRECT")).alias("resolved_value_2"),
    )
)

scored = (
    valid
    .select(
        "id",
        "created_at",
        "value",
        "resolved_value",
        "days_since_created_at",
        "value_length",
        F.dense_rank().over(w).alias("days_since_created_at_dense_rank"),
        F.percent_rank().over(w2).alias("days_since_created_at_percent_rank"),
        F.max("days_since_created_at").over(w3).alias("days_since_created_at_max_1h"),
    )
)

result = (
    scored
    .select(
        "created_at",
        "resolved_value",
        "days_since_created_at",
        "value_length",
        "days_since_created_at_dense_rank",
    )
)
