from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("end_date", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "end_date",
        "monthly_fee",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_5"),
        F.lag("monthly_fee", 1, 0).over(w2).alias("prev_monthly_fee"),
    )
)
