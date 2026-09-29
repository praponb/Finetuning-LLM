from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("monthly_fee", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.first("monthly_fee", ignorenulls=True).over(w).alias("first_known_monthly_fee"),
        F.lag("monthly_fee", 2).over(w2).alias("prev_monthly_fee"),
    )
)
