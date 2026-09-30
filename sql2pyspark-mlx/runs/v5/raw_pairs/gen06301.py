from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 150)
    .select(
        "id",
        "status",
        "start_date",
        "monthly_fee",
        F.first("monthly_fee").over(w).alias("first_monthly_fee"),
    )
)
