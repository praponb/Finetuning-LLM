from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "status").orderBy("monthly_fee", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") < 230)
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        F.avg("monthly_fee").over(w).alias("last3_avg_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last3_max_monthly_fee"),
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
        F.ntile(5).over(w2).alias("monthly_fee_bucket"),
    )
)
