from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("monthly_fee", "id")
w2 = Window.partitionBy("status", "customer_id")

result = (
    df_subscriptions
    .filter(F.col("monthly_fee") <= 90)
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.lead("monthly_fee").over(w_part).alias("next_monthly_fee"),
        F.lag("monthly_fee").over(w_part).alias("prev_monthly_fee"),
        F.row_number().over(w_part).alias("event_seq"),
        F.avg("monthly_fee").over(w2).alias("status_avg_monthly_fee"),
    )
)
