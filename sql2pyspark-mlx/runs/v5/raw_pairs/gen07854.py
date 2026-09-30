from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id", "status")

result = (
    df_subscriptions
    .select(
        "id",
        "plan",
        "status",
        F.min("monthly_fee").over(w_part).alias("running_min_monthly_fee"),
        F.avg("monthly_fee").over(w_part).alias("running_avg_monthly_fee"),
        F.max("monthly_fee").over(w_part).alias("running_max_monthly_fee"),
        F.count("monthly_fee").over(w_part).alias("running_count_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)
