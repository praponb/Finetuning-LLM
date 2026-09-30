from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("status", "customer_id")

with_windows = (
    df_subscriptions
    .withColumn("monthly_fee_avg_3", F.avg("monthly_fee").over(w))
    .withColumn("monthly_fee_share", F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4))
)

result = (
    with_windows
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee_avg_3",
        "monthly_fee_share",
        F.when(F.col("plan").rlike("^(premium|basic)$"), "OTHER_ZONE").otherwise("PRIORITY").alias("plan_zone"),
    )
)
