from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("start_date", "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status")
w5 = Window.partitionBy("plan")

metrics = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 220)
    .select(
        "id",
        "customer_id",
        "end_date",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w2).alias("last_known_monthly_fee"),
        F.row_number().over(w3).alias("row_seq"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w4)), 4).alias("monthly_fee_share"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w5)), 4).alias("monthly_fee_share_2"),
    )
)

result = (
    metrics
    .withColumn("quarter_start", F.date_trunc("quarter", "end_date"))
    .groupingSets(
        [["customer_id", "quarter_start"], ["customer_id"], []],
        "customer_id",
        "quarter_start",
    )
    .agg(
        F.avg("monthly_fee_share_2").alias("avg_monthly_fee_share_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("monthly_fee_share", [0.25, 0.5, 0.75]).alias("monthly_fee_share_quartiles"),
        F.percentile_approx("row_seq", 0.95).alias("row_seq_p95"),
        F.min("monthly_fee_share").alias("min_monthly_fee_share"),
        F.grouping("customer_id").alias("gid"),
    )
    .orderBy(F.col("customer_id").desc_nulls_last())
)
