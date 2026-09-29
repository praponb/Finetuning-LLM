from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([4, 22]) & F.col("unit_price").isNotNull())
)
