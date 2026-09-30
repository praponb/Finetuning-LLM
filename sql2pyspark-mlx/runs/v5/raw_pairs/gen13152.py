from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy(F.col("monthly_fee").desc_nulls_last())
w2 = Window.partitionBy("plan", "status")
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_last())
w4 = Window.partitionBy("plan").orderBy("monthly_fee_share", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_dense_rank").desc(), "id")
w6 = Window.partitionBy("plan").orderBy(F.col("monthly_fee_cume_dist").desc_nulls_last(), "id")
w_running = Window.orderBy("last4_max_monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w_ordered = Window.orderBy("last4_min_monthly_fee", "id")

valid = (
    df_subscriptions
    .filter((F.col("start_date") > F.to_date(F.lit("2023-05-22"))) & F.col("end_date").between("2024-01-06", "2024-07-01"))
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.trim("plan").alias("clean_plan"),
    )
)

scored = (
    valid
    .select(
        "id",
        "plan",
        "monthly_fee",
        "clean_plan",
        F.dense_rank().over(w_recent).alias("monthly_fee_dense_rank"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
    )
)

scored_subscriptions = (
    scored
    .filter(F.col("monthly_fee_cume_dist") >= 0.31)
    .withColumn("last4_max_monthly_fee", F.max("monthly_fee").over(w4))
    .withColumn("last4_min_monthly_fee", F.min("monthly_fee").over(w4))
    .withColumn("last4_count_monthly_fee", F.count("monthly_fee").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

deduped = (
    scored_subscriptions
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "monthly_fee_cume_dist",
        "last4_max_monthly_fee",
        "last4_min_monthly_fee",
    )
)

scored_subscriptions_2 = (
    deduped
    .select(
        "id",
        "last4_max_monthly_fee",
        F.count("monthly_fee_cume_dist").over(w_running).alias("last4_count_monthly_fee_cume_dist"),
        F.max("monthly_fee_cume_dist").over(w_running).alias("last4_max_monthly_fee_cume_dist"),
        F.avg("monthly_fee_cume_dist").over(w_running).alias("last4_avg_monthly_fee_cume_dist"),
        F.ntile(10).over(w_ordered).alias("last4_min_monthly_fee_bucket"),
    )
)

result = (
    scored_subscriptions_2
    .groupBy(
        F.when(F.col("last4_min_monthly_fee_bucket") >= 7, "LARGE").otherwise("SMALL").alias("last4_min_monthly_fee_bucket_tier"),
        F.when(F.col("last4_max_monthly_fee") >= 193, "HIGH").otherwise("LOW").alias("last4_max_monthly_fee_tier"),
    )
    .agg(
        F.round(F.avg("last4_avg_monthly_fee_cume_dist"), 2).alias("avg_last4_avg_monthly_fee_cume_dist"),
        F.count("*").alias("row_count"),
        F.percentile_approx("last4_max_monthly_fee_cume_dist", 0.95).alias("last4_max_monthly_fee_cume_dist_p95"),
    )
)
