from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("customer_id", "plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w_recent).alias("running_avg_monthly_fee"),
        F.max("monthly_fee").over(w_recent).alias("running_max_monthly_fee"),
        F.sum("monthly_fee").over(w_recent).alias("running_sum_monthly_fee"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
    )
)
