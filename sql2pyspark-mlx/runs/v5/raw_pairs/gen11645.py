from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency")
w2 = Window.orderBy(F.col("val").asc_nulls_first())
w3 = Window.partitionBy("currency").orderBy(F.col("val").desc_nulls_last())
w4 = Window.partitionBy("currency").orderBy(F.col("currency_sum_val").desc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("val").desc())
w6 = Window.orderBy(F.col("currency_sum_val_percent_rank").asc_nulls_first())
w_hist = Window.partitionBy("plan").orderBy(F.col("val_rank_share").desc_nulls_first())
w8 = Window.partitionBy("plan").orderBy(F.col("currency_sum_val_percent_rank").desc_nulls_first())

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

metrics = (
    flattened
    .withColumn("currency_max_val", F.max("val").over(w))
    .withColumn("val_rank", F.rank().over(w2))
    .withColumn("val_percent_rank", F.percent_rank().over(w3))
    .withColumn("currency_sum_val", F.sum("val").over(w))
)

with_windows = (
    metrics
    .select(
        "id",
        "plan",
        "effective_from",
        "val",
        "val_rank",
        "currency_sum_val",
        F.percent_rank().over(w4).alias("currency_sum_val_percent_rank"),
        F.dense_rank().over(w5).alias("val_dense_rank"),
        F.round((F.col("val_rank") / F.sum("val_rank").over(w)), 4).alias("val_rank_share"),
    )
)

scored = (
    with_windows
    .select(
        "*",
        F.dense_rank().over(w6).alias("currency_sum_val_percent_rank_dense_rank"),
        F.dense_rank().over(w_hist).alias("val_rank_share_dense_rank"),
        F.percent_rank().over(w_hist).alias("val_rank_share_percent_rank"),
        F.dense_rank().over(w8).alias("currency_sum_val_percent_rank_dense_rank_2"),
    )
)

summary = (
    scored
    .groupBy("plan")
    .agg(
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_count"),
        F.countDistinct("plan").alias("distinct_plans"),
        F.count("*").alias("num_rows"),
        F.size(F.collect_set("plan")).alias("num_plans"),
    )
)

result = (
    summary
    .select(
        "plan",
        "distinct_plans",
        "num_rows",
        "num_plans",
        F.when(F.col("plan").rlike("^(starter|basic)$"), "GROUP_A").when(F.col("plan").rlike("^(pro|enterprise)$"), "PRIORITY").otherwise("GROUP_B").alias("plan_zone"),
    )
    .orderBy(F.col("plan_zone").desc_nulls_first(), F.col("num_plans").desc())
)
