from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("created_at_quarter_filled")
w_grp = Window.orderBy(F.col("created_at_quarter_filled").desc_nulls_first())
w3 = Window.orderBy(F.col("created_at_quarter").asc_nulls_last())

lines = (
    df_carts
    .select(
        "id",
        "created_at",
        "coupon_codes",
        F.explode_outer("attributes").alias("k", "v"),
    )
)

filtered = (
    lines
    .filter(F.col("created_at") > F.to_timestamp(F.lit("2025-03-01 14:00:00")))
    .select("coupon_codes", F.quarter("created_at").alias("created_at_quarter"))
)

valid = (
    filtered
    .filter(F.array_contains("coupon_codes", "BUNDLE"))
    .select(
        "*",
        F.coalesce("created_at_quarter", F.lit(0)).alias("created_at_quarter_filled"),
        F.when(F.col("created_at_quarter") >= 64, "High").when(F.col("created_at_quarter") >= 23, "Medium").otherwise("Low").alias("created_at_quarter_band"),
    )
)

metrics = (
    valid
    .select(
        "coupon_codes",
        "created_at_quarter",
        "created_at_quarter_band",
        F.dense_rank().over(w).alias("created_at_quarter_filled_dense_rank"),
        F.dense_rank().over(w_grp).alias("created_at_quarter_filled_dense_rank_2"),
        F.dense_rank().over(w3).alias("created_at_quarter_dense_rank"),
    )
)

result = (
    metrics
    .select(
        "coupon_codes",
        "created_at_quarter",
        "created_at_quarter_filled_dense_rank",
    )
)
