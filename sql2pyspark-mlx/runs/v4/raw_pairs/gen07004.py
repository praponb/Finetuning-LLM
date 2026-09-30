from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([14, 19, 21]) & (F.col("quantity") < 6))
)
