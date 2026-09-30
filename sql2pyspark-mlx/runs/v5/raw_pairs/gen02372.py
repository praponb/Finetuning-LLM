from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("shipping_fee").between(25, 27))
    .groupBy(F.year("order_date").alias("order_year"))
    .agg(
        F.count("amount").alias("amount_count"),
        F.min("discount").alias("min_discount"),
    )
)
