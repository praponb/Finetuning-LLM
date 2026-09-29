from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status")
w2 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy(F.col("monthly_fee").desc_nulls_first())
w4 = Window.partitionBy("plan").orderBy(F.col("monthly_fee").desc_nulls_last())

metrics = (
    df_subscriptions
    .filter(F.col("start_date") < "2024-09-01")
    .select(
        "id",
        "start_date",
        "end_date",
        F.avg("monthly_fee").over(w).alias("status_avg_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w2).alias("first_known_monthly_fee"),
        F.avg("monthly_fee").over(w).alias("status_avg_monthly_fee_2"),
        F.cume_dist().over(w3).alias("monthly_fee_cume_dist"),
        F.rank().over(w4).alias("monthly_fee_rank"),
    )
)

result = metrics.select("id", "end_date", "monthly_fee_cume_dist", "monthly_fee_rank")
