from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("currency").orderBy(F.col("val").desc())
w2 = Window.partitionBy("plan")

flattened = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode_outer("tier_prices").alias("val"),
    )
)

scored = (
    flattened
    .withColumn("val_dense_rank", F.dense_rank().over(w_recent))
    .withColumn("plan_sum_val", F.sum("val").over(w2))
)

base = (
    scored
    .filter(F.col("plan_sum_val") == 346)
    .select(
        "id",
        "effective_from",
        "currency",
        "val",
        "val_dense_rank",
        F.initcap(F.trim("plan")).alias("plan_title"),
        F.concat("plan", F.lit(", "), "currency").alias("plan_currency"),
    )
)

filtered = (
    base
    .filter(F.col("effective_from") >= F.lit("2024-02-23").cast("date"))
    .select("*", F.upper("currency").alias("currency_upper"))
)

result = (
    filtered
    .groupBy(
        "currency",
        F.when(F.col("val_dense_rank") >= 8, "PREMIUM").otherwise("STANDARD").alias("val_dense_rank_tier"),
    )
    .agg(
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows"),
        F.sort_array(F.collect_set("currency")).alias("currency_list"),
        F.count(F.when(F.col("currency") == "USD", 1)).alias("usd_rows_2"),
        F.count("*").alias("event_count"),
        F.percentile_approx("val", [0.25, 0.5, 0.75]).alias("val_quartiles"),
        F.countDistinct("currency").alias("distinct_currencies"),
    )
    .filter(F.col("event_count") >= 1)
    .orderBy(F.col("event_count").asc_nulls_first())
)
