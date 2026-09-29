from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("plan_sum_monthly_fee"),
    )
)
