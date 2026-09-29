from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "plan").orderBy("monthly_fee", "id").rowsBetween(-3, -1)
w2 = Window.orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan").orderBy("end_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("customer_id").orderBy("monthly_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

result = (
    df_subscriptions
    .filter(F.col("start_date").between("2024-04-24", "2025-05-01"))
    .select(
        "id",
        "customer_id",
        "status",
        F.avg("monthly_fee").over(w).alias("monthly_fee_avg_3_prior"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w3).alias("first_known_monthly_fee"),
        F.first("monthly_fee", ignorenulls=True).over(w_ordered).alias("first_known_monthly_fee_2"),
    )
)
