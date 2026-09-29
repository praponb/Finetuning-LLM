from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("monthly_fee", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "*",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_6"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
    )
)
