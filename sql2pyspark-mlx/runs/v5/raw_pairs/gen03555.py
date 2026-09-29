from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "status",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
    )
    .orderBy("status")
)
