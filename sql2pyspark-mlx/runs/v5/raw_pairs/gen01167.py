from pyspark.sql import functions as F

result = (
    df_order_items
    .select(F.lit("order_item").alias("source"), F.col("unit_price").alias("value"))
    .unionByName(
        df_payments
        .filter(F.col("method") == "paypal")
        .select(F.lit("payment").alias("source"), F.col("amount").alias("value"))
    )
)
