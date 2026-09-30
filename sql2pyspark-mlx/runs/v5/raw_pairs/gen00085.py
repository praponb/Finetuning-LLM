from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")
w2 = Window.partitionBy("customer_id").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("status").isin(["PAUSED", "EXPIRED"]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        "end_date",
        F.avg("monthly_fee").over(w).alias("plan_avg_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)
