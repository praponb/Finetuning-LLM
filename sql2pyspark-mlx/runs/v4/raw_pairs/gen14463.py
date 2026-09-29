from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").desc_nulls_first())
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.orderBy(F.col("val_dense_rank").asc_nulls_first())

flattened = df_price_tiers.select("id", "effective_from", F.explode("tier_prices").alias("val"))

cleaned = (
    flattened
    .filter(F.col("effective_from") > F.lit("2024-04-29").cast("date"))
    .select(
        "*",
        F.date_format("effective_from", "MMM yyyy").alias("effective_from_label"),
    )
)

filtered = (
    cleaned
    .filter(F.col("val") >= 59)
    .select("*", F.date_trunc("MONTH", "effective_from").alias("effective_from_period"))
)

with_windows = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w).alias("val_dense_rank"),
        F.percent_rank().over(w).alias("val_percent_rank"),
        F.rank().over(w2).alias("val_rank"),
    )
)

with_windows_price_tiers = (
    with_windows
    .withColumn("val_rank_max_30d", F.max("val_rank").over(w3))
    .withColumn("val_dense_rank_rank", F.rank().over(w4))
)

result = (
    with_windows_price_tiers
    .select(
        "id",
        "effective_from_label",
        "effective_from_period",
        "val_percent_rank",
        "val_rank",
        "val_dense_rank_rank",
    )
    .orderBy("val_dense_rank_rank")
)
