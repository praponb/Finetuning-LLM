from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy(F.col("monthly_fee").desc_nulls_last())

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "end_date",
        "status",
        F.max("monthly_fee").over(w_part).alias("running_max_monthly_fee"),
        F.sum("monthly_fee").over(w_part).alias("running_sum_monthly_fee"),
        F.min("monthly_fee").over(w_part).alias("running_min_monthly_fee"),
        F.percent_rank().over(w_ordered).alias("monthly_fee_percent_rank"),
    )
)
