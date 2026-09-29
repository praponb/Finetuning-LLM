from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("start_date", "id")
w3 = Window.partitionBy("plan").orderBy(F.col("start_date").desc(), "id")
monthly_fee_by_customer_id = df_subscriptions.groupBy("customer_id").agg(F.max("monthly_fee").alias("max_subscription_monthly_fee"))

enriched = (
    df_subscriptions
    .withColumn("monthly_fee_avg_5", F.avg("monthly_fee").over(w))
    .withColumn("next_monthly_fee", F.lead("monthly_fee", 1).over(w2))
    .withColumn("event_seq", F.row_number().over(w3))
)

result = (
    enriched
    .join(monthly_fee_by_customer_id, on="customer_id", how="left")
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        "monthly_fee_avg_5",
        "next_monthly_fee",
        "event_seq",
        F.when(F.col("plan").rlike("^(basic|standard)$"), "GROUP_B").when(F.col("plan").rlike("^(premium|enterprise)$"), "PRIORITY").otherwise("EXTENDED").alias("plan_zone"),
        F.col("max_subscription_monthly_fee"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
