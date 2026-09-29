from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("plan", "customer_id")
w3 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .select(
        "id",
        "customer_id",
        "plan",
        "end_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("cumulative_monthly_fee"),
        F.round((F.col("monthly_fee") / F.sum("monthly_fee").over(w2)), 4).alias("monthly_fee_share"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
    )
)
