from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("customer_id", "plan")
w_grp = Window.partitionBy("customer_id")
w_recent = Window.partitionBy("plan")
w_ordered = Window.orderBy(F.col("monthly_fee_share").desc())

filtered = (
    df_subscriptions
    .filter((F.col("start_date") > F.lit("2023-12-30").cast("date")) & F.col("customer_id").isNotNull() & (F.col("end_date") <= "2025-08-01"))
    .withColumn("status_title", F.initcap(F.trim("status")))
)

kept_keys = (
    filtered
    .select("customer_id", "id")
    .intersect(
        df_subscriptions
        .filter(F.col("start_date") >= "2023-05-12")
        .select("customer_id", "id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.customer_id") == F.col("k.customer_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

metrics = (
    verified
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w)), 4))
    .withColumn("status_avg_monthly_fee", F.avg("monthly_fee").over(w))
    .withColumn("customer_id_sum_monthly_fee", F.sum("monthly_fee").over(w2))
)

result = (
    metrics
    .select(
        "*",
        F.count("monthly_fee_share").over(w_grp).alias("customer_id_count_monthly_fee_share"),
        F.stddev_samp("customer_id_sum_monthly_fee").over(w_recent).alias("plan_stddev_samp_customer_id_sum_monthly_fee"),
        F.dense_rank().over(w_ordered).alias("monthly_fee_share_dense_rank"),
    )
)
