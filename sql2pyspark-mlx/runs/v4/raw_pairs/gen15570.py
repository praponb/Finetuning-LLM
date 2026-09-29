from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("start_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "start_date",
        "monthly_fee",
        "status",
        F.first("monthly_fee").over(w).alias("first_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 3).alias("monthly_fee_share"),
    )
)
