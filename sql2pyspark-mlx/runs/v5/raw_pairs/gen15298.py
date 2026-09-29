from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id")

result = (
    df_subscriptions
    .filter(F.col("start_date") <= "2024-03-26")
    .select(
        "id",
        "customer_id",
        "start_date",
        "monthly_fee",
        F.sum("monthly_fee").over(w).alias("running_monthly_fee"),
        F.sum("monthly_fee").over(w2).alias("running_monthly_fee_2"),
    )
    .orderBy(F.col("customer_id").desc(), F.col("running_monthly_fee").desc())
)
