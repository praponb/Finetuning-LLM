from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id")
w2 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan").orderBy("start_date_minus_6d", "id")
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_share").desc_nulls_last())
w_part = Window.partitionBy("status", "customer_id").orderBy("start_date_minus_6d", "id")
w_running = Window.partitionBy("customer_id").orderBy(F.col("prev_monthly_fee").desc_nulls_last())
w_hist = Window.partitionBy("plan")
w8 = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").asc_nulls_first())

base = (
    df_subscriptions
    .filter(F.col("end_date") >= F.to_date(F.lit("2023-08-29")))
    .select("*", F.date_sub("start_date", 6).alias("start_date_minus_6d"))
)

with_windows = (
    base
    .select(
        "*",
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4).alias("monthly_fee_share"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 2, 0).over(w3).alias("prev_monthly_fee_2"),
    )
)

scored = (
    with_windows
    .withColumn("monthly_fee_share_percent_rank", F.percent_rank().over(w4))
    .withColumn("next_prev_monthly_fee_2", F.lead("prev_monthly_fee_2", 1).over(w_part))
    .withColumn("prev_monthly_fee_dense_rank", F.dense_rank().over(w_running))
)

remaining_keys = (
    scored
    .select("id", "plan")
    .subtract(
        df_subscriptions
        .filter(F.col("customer_id").isNotNull())
        .select("id", "plan")
    )
)

kept_rows = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.plan") == F.col("k.plan")), "inner")
    .select("s.*")
)

enriched = (
    kept_rows
    .select(
        "*",
        F.round((F.col("next_prev_monthly_fee_2") / F.sum("next_prev_monthly_fee_2").over(w)), 4).alias("next_prev_monthly_fee_2_share"),
        F.min("next_prev_monthly_fee_2").over(w_hist).alias("plan_min_next_prev_monthly_fee_2"),
        F.min("prev_monthly_fee").over(w_hist).alias("plan_min_prev_monthly_fee"),
        F.dense_rank().over(w8).alias("monthly_fee_dense_rank"),
    )
)

by_type = (
    enriched
    .groupBy("status")
    .pivot("plan", ["enterprise", "basic"])
    .agg(
        F.avg("monthly_fee_dense_rank").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)

result = (
    by_type
    .select(
        "status",
        "enterprise_amount",
        "basic_amount",
        "basic_cnt",
        F.when(F.col("status").rlike("^(CANCELLED|EXPIRED)$"), "GROUP_A").when(F.col("status").rlike("^(ACTIVE|PAUSED)$"), "GROUP_B").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("basic_cnt", F.col("status_zone").desc())
)
