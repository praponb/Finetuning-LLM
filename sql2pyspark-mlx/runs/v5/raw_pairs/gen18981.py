from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("status_length").desc(), "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("plan", "customer_id").orderBy("status_length", "id")

base = (
    df_subscriptions
    .filter(F.col("end_date") > F.lit("2023-07-25").cast("date"))
    .select("*", F.length("status").alias("status_length"))
)

result = (
    base
    .filter(F.col("customer_id").isin([16, 40]))
    .select(
        "id",
        "customer_id",
        "plan",
        "status",
        F.ntile(5).over(w).alias("status_length_bucket"),
        F.sum("monthly_fee").over(w2).alias("cumulative_monthly_fee"),
        F.ntile(4).over(w3).alias("status_length_bucket_2"),
    )
)
