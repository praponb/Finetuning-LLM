from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("plan").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.unboundedFollowing)
w2 = Window.partitionBy("status", "plan").orderBy("end_date", "id").rowsBetween(-3, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("plan") == "basic")
    .select(
        "id",
        "plan",
        "monthly_fee",
        "customer_id",
        "status",
        "end_date",
        F.last("customer_id").over(w).alias("last_customer_id"),
        F.round(F.avg("monthly_fee").over(w2), 2).alias("monthly_fee_moving_avg_4"),
    )
)
