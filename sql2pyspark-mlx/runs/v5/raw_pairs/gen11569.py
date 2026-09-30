from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("plan").orderBy(F.col("monthly_fee_whole").asc_nulls_first())
w2 = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee_whole").desc())
w3 = Window.partitionBy("plan")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_whole_filled").desc())
w5 = Window.partitionBy("plan").orderBy("monthly_fee_whole_percent_rank")
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_whole_cume_dist").desc_nulls_first())
w7 = Window.orderBy(F.col("plan_max_monthly_fee_whole_percent_rank").desc())

filtered = (
    df_subscriptions
    .filter((F.col("start_date") >= F.to_date(F.lit("2023-05-10"))) & F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

kept_keys = (
    filtered
    .select("plan", "customer_id")
    .subtract(
        df_subscriptions
        .filter(F.col("status").isin(["EXPIRED", "PAUSED"]))
        .select("plan", "customer_id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter((F.col("end_date") > F.lit("2023-09-29").cast("date")) & F.col("customer_id").isNotNull())
    .select(
        "*",
        F.coalesce("monthly_fee_whole", F.lit(0)).alias("monthly_fee_whole_filled"),
        F.add_months("end_date", 6).alias("end_date_plus_months"),
    )
)

with_windows = (
    cleaned
    .filter(F.col("monthly_fee") <= 90)
    .select(
        "id",
        "plan",
        "end_date",
        "monthly_fee_whole_filled",
        "end_date_plus_months",
        F.cume_dist().over(w_grp).alias("monthly_fee_whole_cume_dist"),
        F.percent_rank().over(w2).alias("monthly_fee_whole_percent_rank"),
    )
)

with_windows_subscriptions = (
    with_windows
    .withColumn("plan_max_monthly_fee_whole_percent_rank", F.max("monthly_fee_whole_percent_rank").over(w3))
    .withColumn("monthly_fee_whole_filled_dense_rank", F.dense_rank().over(w4))
    .withColumn("plan_avg_monthly_fee_whole_filled", F.avg("monthly_fee_whole_filled").over(w3))
    .withColumn("monthly_fee_whole_percent_rank_cume_dist", F.cume_dist().over(w5))
)

with_windows_subscriptions_2 = (
    with_windows_subscriptions
    .select(
        "*",
        F.percent_rank().over(w6).alias("monthly_fee_whole_cume_dist_percent_rank"),
        F.sum("monthly_fee_whole_percent_rank_cume_dist").over(w3).alias("plan_sum_monthly_fee_whole_percent_rank_cume_dist"),
        F.percent_rank().over(w7).alias("plan_max_monthly_fee_whole_percent_rank_percent_rank"),
    )
)

cleaned_subscriptions = (
    with_windows_subscriptions_2
    .filter(F.col("monthly_fee_whole_cume_dist") > 0.7)
    .select(
        "*",
        F.when(F.col("plan_max_monthly_fee_whole_percent_rank_percent_rank") >= 0.66, "High").when(F.col("plan_max_monthly_fee_whole_percent_rank_percent_rank") >= 0.21, "Medium").otherwise("Low").alias("plan_max_monthly_fee_whole_percent_rank_percent_rank_tier"),
    )
)

result = (
    cleaned_subscriptions
    .groupBy(F.date_trunc("quarter", "end_date_plus_months").alias("quarter_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count(F.when(F.col("plan") == "basic", 1)).alias("basic_rows"),
        F.round(F.avg("monthly_fee_whole_cume_dist"), 2).alias("avg_monthly_fee_whole_cume_dist"),
    )
    .filter(F.col("basic_rows") < 1000)
    .orderBy(F.col("distinct_ids").desc())
)
