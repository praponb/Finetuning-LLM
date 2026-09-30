from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        "status",
        F.when(F.col("amount") >= 1610, "high").when(F.col("amount") >= 440, "mid").otherwise("low").alias("amount_bucket"),
    )
    .agg(
        (F.max("discount") - F.min("discount")).alias("discount_range"),
        F.max("order_date").alias("last_order_date"),
    )
    .filter(F.col("discount_range") > 61)
    .orderBy("status", "amount_bucket")
    .limit(10)
)
