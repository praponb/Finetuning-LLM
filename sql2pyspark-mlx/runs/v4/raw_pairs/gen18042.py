from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "currency")
w_recent = Window.partitionBy("plan").orderBy("val")
w_hist = Window.partitionBy("plan")
w4 = Window.partitionBy("currency")
w5 = Window.partitionBy("plan").orderBy(F.col("plan_count_val").desc_nulls_last())
w_grp = Window.orderBy("val")
w7 = Window.orderBy(F.col("val").asc_nulls_last())
w8 = Window.orderBy("val_dense_rank")
w9 = Window.orderBy(F.col("val_dense_rank").desc())
w10 = Window.orderBy(F.col("val_cume_dist").desc_nulls_last())
w11 = Window.orderBy(F.col("val_dense_rank").asc_nulls_first())
w12 = Window.orderBy(F.col("val").desc_nulls_last())

exploded = (
    df_price_tiers
    .select("id", "plan", "currency", F.explode_outer("tier_prices").alias("val"))
)

enriched = (
    exploded
    .withColumn("plan_count_val", F.count("val").over(w))
    .withColumn("val_percent_rank", F.percent_rank().over(w_recent))
    .withColumn("plan_max_val", F.max("val").over(w_hist))
)

cleaned = (
    enriched
    .filter(F.col("currency").isin(["THB", "EUR"]))
    .withColumn("plan_count_val_times_val_percent_rank", (F.col("plan_count_val") * F.col("val_percent_rank")))
    .withColumn("plan_upper", F.upper("plan"))
)

metrics = (
    cleaned
    .withColumn("currency_stddev_samp_val_percent_rank", F.stddev_samp("val_percent_rank").over(w4))
    .withColumn("plan_count_val_cume_dist", F.cume_dist().over(w5))
)

filtered = (
    metrics
    .filter(F.col("plan_count_val_times_val_percent_rank").between(44, 57))
    .select(
        "id",
        "val",
        "plan_upper",
        F.when(F.col("currency_stddev_samp_val_percent_rank") >= 1, "High").when(F.col("currency_stddev_samp_val_percent_rank") >= 0, "Medium").otherwise("Low").alias("currency_stddev_samp_val_percent_rank_band"),
        F.substring("currency", 1, 2).alias("currency_prefix"),
    )
)

metrics_price_tiers = (
    filtered
    .select(
        "*",
        F.dense_rank().over(w_grp).alias("val_dense_rank"),
        F.dense_rank().over(w_grp).alias("val_dense_rank_2"),
        F.cume_dist().over(w7).alias("val_cume_dist"),
    )
)

scored = (
    metrics_price_tiers
    .select(
        "*",
        F.percent_rank().over(w8).alias("val_dense_rank_percent_rank"),
        F.cume_dist().over(w9).alias("val_dense_rank_cume_dist"),
        F.percent_rank().over(w10).alias("val_cume_dist_percent_rank"),
        F.rank().over(w11).alias("val_dense_rank_rank"),
        F.percent_rank().over(w12).alias("val_percent_rank"),
    )
)

result = (
    scored
    .select(
        "val",
        "plan_upper",
        "currency_prefix",
        "val_cume_dist",
        "val_dense_rank_percent_rank",
        "val_dense_rank_cume_dist",
        "val_cume_dist_percent_rank",
        "val_percent_rank",
    )
)
