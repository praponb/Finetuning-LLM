from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("status")
w3 = Window.partitionBy("customer_id")
w4 = Window.partitionBy("plan", "status").orderBy("end_date", "id")
w5 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "*",
        F.percent_rank().over(w).alias("monthly_fee_percent_rank"),
        F.avg("monthly_fee").over(w2).alias("status_avg_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w3)), 4).alias("monthly_fee_share"),
        F.lead("monthly_fee", 2).over(w4).alias("next_monthly_fee"),
        F.last("monthly_fee", ignorenulls=True).over(w5).alias("last_known_monthly_fee"),
    )
)
