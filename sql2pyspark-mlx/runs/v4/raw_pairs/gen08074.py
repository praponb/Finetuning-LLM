from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("plan").orderBy("monthly_fee", "id")
w_hist = Window.orderBy("end_date", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.ntile(10).over(w).alias("monthly_fee_bucket"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket_2"),
        F.row_number().over(w_hist).alias("event_seq"),
    )
)
