from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date") > "2024-10-01")
    .select(
        "id",
        "status",
        "end_date",
        "monthly_fee",
        F.lag("monthly_fee", 1).over(w).alias("prev_monthly_fee"),
    )
    .orderBy(F.col("id").desc())
)
