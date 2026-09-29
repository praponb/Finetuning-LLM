from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")
w2 = Window.partitionBy("status").orderBy("start_date", "id")

with_windows = (
    df_subscriptions
    .select(
        "*",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
        F.lag("monthly_fee", 1, 0).over(w2).alias("prev_monthly_fee_2"),
    )
)

result = (
    with_windows
    .select(
        "id",
        "customer_id",
        "start_date",
        "end_date",
        "status",
        "prev_monthly_fee_2",
        F.when(F.col("status").rlike("^(CANCELLED|PAUSED)$"), "CORE").otherwise("EXTENDED").alias("status_zone"),
    )
)
