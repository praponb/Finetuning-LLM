from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("monthly_fee", "id")

metrics = (
    df_subscriptions
    .select(
        "id",
        "status",
        F.lag("monthly_fee").over(w).alias("prev_monthly_fee"),
        F.lead("monthly_fee").over(w).alias("next_monthly_fee"),
        F.row_number().over(w).alias("row_seq"),
        F.ntile(10).over(w2).alias("monthly_fee_bucket"),
    )
)

result = metrics.select("id", "prev_monthly_fee", "monthly_fee_bucket")
