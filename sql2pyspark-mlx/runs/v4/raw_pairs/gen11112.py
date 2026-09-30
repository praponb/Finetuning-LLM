from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("start_date").desc(), "id", "subscription_id")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id", "subscription_id").rowsBetween(-2, -1)
w3 = Window.partitionBy("status").orderBy("subscription_monthly_fee", "id", "subscription_id")
w4 = Window.partitionBy("plan").orderBy("monthly_fee")
w5 = Window.orderBy("subscription_id", "id", "subscription_id")
w6 = Window.partitionBy("status", "customer_id").orderBy("monthly_fee_dense_rank", "id", "subscription_id")
w7 = Window.partitionBy("status", "customer_id").orderBy("subscription_id_avg_2_prior", "id", "subscription_id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("status").orderBy("credit_limit", "id", "subscription_id")

enriched = (
    df_subscriptions
    .alias("s")
    .join(df_customers.alias("p"), F.col("s.customer_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.credit_limit"))
)

matched = (
    enriched
    .alias("src")
    .join(df_subscriptions.alias("sub"), (F.col("src.customer_id") == F.col("sub.customer_id")) & F.col("src.start_date").between(F.col("sub.start_date"), F.col("sub.end_date")), "inner")
    .select(
        "src.*",
        F.col("sub.id").alias("subscription_id"),
        F.col("sub.plan").alias("subscription_plan"),
        F.col("sub.monthly_fee").alias("subscription_monthly_fee"),
    )
)

metrics = (
    matched
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("subscription_id_avg_2_prior", F.avg("subscription_id").over(w2))
    .withColumn("prev_subscription_monthly_fee", F.lag("subscription_monthly_fee", 1).over(w3))
    .withColumn("monthly_fee_dense_rank", F.dense_rank().over(w4))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 2, 0).over(w5))
)

result = (
    metrics
    .select(
        "*",
        F.lag("next_monthly_fee", 1).over(w6).alias("prev_next_monthly_fee"),
        F.first("subscription_id", ignorenulls=True).over(w7).alias("first_known_subscription_id"),
        F.ntile(4).over(w8).alias("credit_limit_bucket"),
    )
)
