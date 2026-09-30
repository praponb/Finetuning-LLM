from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") >= 220) & (F.col("quantity") >= 5) & F.col("order_id").isin([26, 34, 36]))
    .select("id", "quantity", F.ceil("unit_price").alias("unit_price_whole"))
)
