from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.lead("monthly_fee", 1).over(w_hist).alias("next_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.sum("monthly_fee").over(w_running).alias("running_monthly_fee"),
    )
)
