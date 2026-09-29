from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "start_date",
        "status",
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
        F.min("monthly_fee").over(w).alias("last3_min_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
    )
)
