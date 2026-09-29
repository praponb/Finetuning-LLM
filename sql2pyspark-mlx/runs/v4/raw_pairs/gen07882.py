from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("status", "customer_id").orderBy(F.col("monthly_fee").desc_nulls_last())
w3 = Window.partitionBy("plan", "customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.sum("monthly_fee").alias("sum_subscription_monthly_fee"))

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_bucket", F.ntile(5).over(w))
    .withColumn("monthly_fee_rank", F.rank().over(w2))
    .withColumn("cumulative_monthly_fee", F.sum("monthly_fee").over(w3))
)

result = (
    with_windows
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "monthly_fee_bucket",
        "monthly_fee_rank",
        "cumulative_monthly_fee",
        F.when(F.col("plan").rlike("^(standard|enterprise)$"), "EXTENDED").when(F.col("plan").rlike("^(basic|premium)$"), "GROUP_B").otherwise("GROUP_A").alias("plan_zone"),
        F.col("sum_subscription_monthly_fee"),
    )
    .orderBy(F.col("plan").desc_nulls_first(), "cumulative_monthly_fee")
)
