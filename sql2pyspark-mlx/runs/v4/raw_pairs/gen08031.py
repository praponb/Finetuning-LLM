from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "monthly_fee",
        F.min("monthly_fee").over(w).alias("plan_min_monthly_fee"),
    )
    .orderBy(F.col("id").desc())
)
