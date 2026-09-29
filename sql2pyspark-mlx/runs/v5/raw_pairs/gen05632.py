from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("end_date", "id")
w2 = Window.orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("event_seq"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee"),
    )
)
