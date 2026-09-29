from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([5, 18, 24, 32]) & F.col("order_id").isin([6, 8, 32]))
    .groupBy("product_id")
    .agg((F.max("quantity") - F.min("quantity")).alias("quantity_range"))
    .filter(F.col("quantity_range") > 76)
)
