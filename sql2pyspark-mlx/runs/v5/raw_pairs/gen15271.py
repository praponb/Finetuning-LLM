from pyspark.sql import functions as F

result = (
    df_subscriptions
    .filter(F.col("status") == "EXPIRED")
    .select("end_date")
    .unionByName(
        df_products
        .filter(F.col("stock_qty").between(129, 329))
        .select(F.col("launch_date").alias("end_date"))
    )
    .distinct()
    .orderBy("end_date")
)
