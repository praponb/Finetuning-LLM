from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("plan").orderBy("end_date", "id")
w2 = Window.partitionBy("plan", "status").orderBy(F.col("monthly_fee").desc(), "id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee").between(200, 230))
    .select(
        "id",
        "customer_id",
        "monthly_fee",
        F.lag("monthly_fee").over(w_running).alias("prev_monthly_fee"),
        F.row_number().over(w_running).alias("seq_no"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
    )
)
