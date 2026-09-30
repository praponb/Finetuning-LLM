from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(190, 240))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "status",
        F.lead("monthly_fee", 1).over(w).alias("next_monthly_fee"),
        F.ntile(4).over(w2).alias("monthly_fee_bucket"),
    )
)
