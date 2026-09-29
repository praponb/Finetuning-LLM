from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("val")
w2 = Window.partitionBy("currency")

lines = (
    df_price_tiers
    .select(
        "id",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

valid = (
    lines
    .filter((F.col("effective_from") > F.to_date(F.lit("2024-04-30"))) & (F.col("val") >= 27))
    .withColumn("effective_from_plus_months", F.add_months("effective_from", 3))
)

metrics = (
    valid
    .withColumn("val_dense_rank", F.dense_rank().over(w))
    .withColumn("currency_sum_val", F.sum("val").over(w2))
)

filtered = (
    metrics
    .filter(F.col("effective_from") > F.lit("2024-04-22").cast("date"))
    .withColumn("currency_sum_val_minus_val_dense_rank", (F.col("currency_sum_val") - F.col("val_dense_rank")))
)

cleaned = (
    filtered
    .filter(F.col("currency") == "USD")
    .select(
        "id",
        "currency",
        "val",
        "val_dense_rank",
        "currency_sum_val",
        "currency_sum_val_minus_val_dense_rank",
        F.floor("val").alias("val_whole"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "currency",
        "val",
        "currency_sum_val",
        "currency_sum_val_minus_val_dense_rank",
        "val_whole",
        F.when(F.col("currency").rlike("^(THB|EUR)$"), "EXTENDED").otherwise("OTHER_ZONE").alias("currency_zone"),
    )
    .orderBy(F.col("val").desc_nulls_first())
)
