from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w2 = Window.orderBy("monthly_fee", "id")
w3 = Window.partitionBy("plan", "customer_id").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("prev_monthly_fee", "id")
w5 = Window.partitionBy("customer_id").orderBy(F.col("age").desc(), "id")
w6 = Window.partitionBy("plan").orderBy("age", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.avg("monthly_fee").alias("avg_subscription_monthly_fee"))

enriched = (
    df_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 2, 0).over(w2).alias("prev_monthly_fee_2"),
    )
)

enriched_subscriptions = (
    enriched
    .alias("s")
    .join(df_customers.alias("d"), F.col("s.customer_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.age"))
)

valid = (
    enriched_subscriptions
    .filter(F.col("prev_monthly_fee_2") > 190)
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "prev_monthly_fee",
        "prev_monthly_fee_2",
        "age",
        F.least("age", "monthly_fee").alias("least_age_monthly_fee"),
    )
)

metrics = (
    valid
    .withColumn("last_known_prev_monthly_fee", F.last("prev_monthly_fee", ignorenulls=True).over(w3))
    .withColumn("next_prev_monthly_fee_2", F.lead("prev_monthly_fee_2", 1).over(w4))
    .withColumn("age_bucket", F.ntile(4).over(w5))
    .withColumn("cumulative_prev_monthly_fee_2", F.sum("prev_monthly_fee_2").over(w6))
    .withColumn("prev_monthly_fee_2_avg_5", F.avg("prev_monthly_fee_2").over(w7))
)

result = (
    metrics
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "end_date",
        "prev_monthly_fee",
        "age",
        "least_age_monthly_fee",
        "last_known_prev_monthly_fee",
        "next_prev_monthly_fee_2",
        "age_bucket",
        F.col("avg_subscription_monthly_fee"),
    )
    .orderBy(F.col("least_age_monthly_fee").desc_nulls_first())
)
