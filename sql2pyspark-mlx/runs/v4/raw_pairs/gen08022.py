from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("status")
w_ordered_2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w3 = Window.orderBy(F.col("monthly_fee").asc_nulls_first())
w4 = Window.orderBy("credit_limit", "id")
w5 = Window.partitionBy("plan").orderBy(F.col("credit_limit").desc(), "id")
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_cume_dist").desc_nulls_first())
w7 = Window.partitionBy("plan")

valid = (
    df_subscriptions
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.floor("monthly_fee").alias("monthly_fee_whole"),
    )
)

joined = (
    valid
    .alias("s")
    .join(df_customers.alias("b"), F.col("s.customer_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.name"), F.col("b.credit_limit"))
)

scored = (
    joined
    .select(
        "*",
        F.stddev_samp("credit_limit").over(w_ordered).alias("status_stddev_samp_credit_limit"),
        F.ntile(3).over(w_ordered_2).alias("monthly_fee_bucket"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
        F.lag("monthly_fee_whole", 1).over(w4).alias("prev_monthly_fee_whole"),
        F.ntile(10).over(w5).alias("credit_limit_bucket"),
    )
)

with_windows = (
    scored
    .select(
        "*",
        F.dense_rank().over(w6).alias("monthly_fee_cume_dist_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w7)), 4).alias("monthly_fee_share"),
    )
)

stats = (
    with_windows
    .cube(
        F.when(F.col("monthly_fee_cume_dist") >= 0.76, "LARGE").otherwise("SMALL").alias("monthly_fee_cume_dist_tier"),
    )
    .agg(
        F.count_if(F.col("plan") == "enterprise").alias("enterprise_events"),
        F.round(F.sum("status_stddev_samp_credit_limit") / F.nullif(F.count(F.when(F.col("plan") == "premium", 1)), F.lit(0)), 2).alias("status_stddev_samp_credit_limit_per_premium"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("status_stddev_samp_credit_limit_per_premium") >= 0)
)

result = (
    stats
    .select(
        "monthly_fee_cume_dist_tier",
        "status_stddev_samp_credit_limit_per_premium",
        "row_count",
    )
    .orderBy(F.col("monthly_fee_cume_dist_tier").asc_nulls_last())
)
