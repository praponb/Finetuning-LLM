from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan", "customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
    )
)
