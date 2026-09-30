from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id")

result = (
    df_subscriptions
    .select(
        "*",
        F.sum("monthly_fee").over(w).alias("last3_sum_monthly_fee"),
        F.max("monthly_fee").over(w).alias("last3_max_monthly_fee"),
        F.min("monthly_fee").over(w).alias("last3_min_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
    )
)
