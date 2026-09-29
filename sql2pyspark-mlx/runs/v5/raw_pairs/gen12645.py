from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").isNotNull())
    .groupBy("channel")
    .agg(
        F.count("*").alias("row_count"),
        F.max("amount").alias("max_amount"),
        (F.max("shipping_fee") - F.min("shipping_fee")).alias("shipping_fee_range"),
    )
    .orderBy("channel")
    .limit(10)
)
