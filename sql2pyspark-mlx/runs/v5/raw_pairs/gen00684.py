from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.max("monthly_fee").over(w).alias("plan_max_monthly_fee"),
        F.max("monthly_fee").over(w).alias("plan_max_monthly_fee_2"),
    )
    .orderBy("id")
)
