from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2023-07-01")
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.avg("monthly_fee").over(w).alias("last4_avg_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last4_max_monthly_fee"),
        F.min("monthly_fee").over(w).alias("last4_min_monthly_fee"),
    )
)
