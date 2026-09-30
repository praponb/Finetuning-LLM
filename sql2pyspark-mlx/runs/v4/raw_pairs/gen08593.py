from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan").orderBy("end_date", "id")
w_running = Window.partitionBy("customer_id").orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.ntile(5).over(w).alias("monthly_fee_bucket"),
        F.lag("monthly_fee", 1, 0).over(w2).alias("prev_monthly_fee"),
        F.row_number().over(w_running).alias("seq_no"),
    )
)
