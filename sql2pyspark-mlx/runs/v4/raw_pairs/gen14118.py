from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("val").asc_nulls_last())
w2 = Window.orderBy(F.col("val").desc_nulls_first())

lines = (
    df_price_tiers
    .select("id", "effective_from", F.explode_outer("discounts").alias("val"))
)

with_windows = (
    lines
    .withColumn("val_rank", F.rank().over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w2))
    .withColumn("val_rank_2", F.rank().over(w))
)

remaining_keys = (
    with_windows
    .select("id")
    .exceptAll(
        df_price_tiers
        .filter(F.col("effective_from") <= "2023-10-01")
        .select("id")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

base = (
    kept_rows
    .filter((F.col("effective_from") >= F.lit("2024-10-29").cast("date")) & F.col("val").between(45, 77))
    .withColumn("val_rank_2_minus_val", (F.col("val_rank_2") - F.col("val")))
)

report = base.select("id", "effective_from", "val", "val_rank_2", "val_rank_2_minus_val")

result = (
    report
    .select("id", "effective_from", "val", "val_rank_2_minus_val")
    .orderBy(F.col("val").asc_nulls_first())
)
