from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc())
w2 = Window.partitionBy("plan").orderBy("end_date", "id")

result = (
    df_subscriptions
    .filter(F.month("start_date").isin([6, 9, 12]))
    .select(
        "id",
        "plan",
        "monthly_fee",
        "end_date",
        F.dense_rank().over(w).alias("monthly_fee_dense_rank"),
        F.lag("monthly_fee", 1).over(w2).alias("prev_monthly_fee"),
    )
)
