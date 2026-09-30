from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("currency").orderBy(F.col("val_adjusted").desc_nulls_first())
w2 = Window.orderBy(F.col("val_whole").desc_nulls_last())

exploded = df_price_tiers.select("id", "plan", "currency", F.explode("discounts").alias("val"))

cleaned = (
    exploded
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("val_whole", F.ceil("val"))
    .withColumn("val_adjusted", F.round((F.col("val") * 1.15), 2))
)

scored = (
    cleaned
    .withColumn("val_adjusted_dense_rank", F.dense_rank().over(w_hist))
    .withColumn("val_whole_dense_rank", F.dense_rank().over(w2))
)

summary = (
    scored
    .withColumn("val_whole_dense_rank_tier", F.when(F.col("val_whole_dense_rank") >= 9, "HIGH").otherwise("LOW"))
    .withColumn("val_whole_tier", F.when(F.col("val_whole") >= 72, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["plan", "val_whole_dense_rank_tier", "val_whole_tier"], ["plan", "val_whole_dense_rank_tier"], []],
        "plan",
        "val_whole_dense_rank_tier",
        "val_whole_tier",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.size(F.collect_set("plan")).alias("num_plans"),
        F.count(F.when(F.col("currency") == "THB", 1)).alias("thb_rows"),
        F.round(F.sum("val_whole") / F.nullif(F.count(F.when(F.col("currency") == "USD", 1)), F.lit(0)), 2).alias("val_whole_per_usd"),
        F.round(F.sum("val_adjusted_dense_rank") / F.nullif(F.count(F.when(F.col("plan") == "pro", 1)), F.lit(0)), 2).alias("val_adjusted_dense_rank_per_pro"),
    )
)

result = (
    summary
    .select(
        "val_whole_tier",
        "row_count",
        "row_count_2",
        "num_plans",
        "thb_rows",
        "val_adjusted_dense_rank_per_pro",
    )
    .orderBy(F.col("num_plans").desc_nulls_first(), "row_count_2")
)
