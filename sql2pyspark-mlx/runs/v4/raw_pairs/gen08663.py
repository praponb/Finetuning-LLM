from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("discount") < 0.19)
    .groupBy("channel")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.max("order_date").alias("latest_order_date"),
    )
    .orderBy(F.col("partner_count").desc())
)
