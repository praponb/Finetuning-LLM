from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("monthly_fee").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("monthly_fee", "id")
w3 = Window.orderBy("monthly_fee", "id")
w4 = Window.partitionBy("status").orderBy("start_date", "id")

result = (
    df_subscriptions
    .select(
        "*",
        F.ntile(4).over(w).alias("monthly_fee_bucket"),
        F.row_number().over(w2).alias("seq_no"),
        F.ntile(3).over(w3).alias("monthly_fee_bucket_2"),
        F.lag("monthly_fee", 1, 0).over(w4).alias("prev_monthly_fee"),
    )
)
