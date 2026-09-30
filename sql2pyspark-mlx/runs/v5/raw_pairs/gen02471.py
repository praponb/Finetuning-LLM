from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("end_date", "id")
w2 = Window.partitionBy("status")

result = (
    df_subscriptions
    .select(
        "id",
        "end_date",
        "monthly_fee",
        "status",
        F.lag("monthly_fee", 2).over(w).alias("prev_monthly_fee"),
        F.max("monthly_fee").over(w2).alias("status_max_monthly_fee"),
    )
)
