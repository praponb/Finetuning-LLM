from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").desc())
w3 = Window.partitionBy("currency").orderBy(F.col("effective_from_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w4 = Window.partitionBy("currency").orderBy(F.col("currency_count_val").desc())

exploded = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("discounts").alias("val"),
    )
)

metrics = (
    exploded
    .select(
        "*",
        F.count("val").over(w_ordered).alias("currency_count_val"),
        F.percent_rank().over(w2).alias("val_percent_rank"),
        F.min("val").over(w_ordered).alias("currency_min_val"),
    )
)

base = (
    metrics
    .filter(F.col("effective_from") >= F.lit("2024-01-30").cast("date"))
    .withColumn("effective_from_period", F.date_trunc("YEAR", "effective_from"))
)

scored = (
    base
    .select(
        "effective_from",
        "currency_count_val",
        "val_percent_rank",
        "currency_min_val",
        F.count("val").over(w3).alias("val_count_30d"),
        F.rank().over(w4).alias("currency_count_val_rank"),
        F.sum("val").over(w_ordered).alias("currency_sum_val"),
    )
)

result = (
    scored
    .select("val_percent_rank", "currency_sum_val")
    .orderBy(F.col("currency_sum_val").asc_nulls_last())
)
