from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("order_id").isin([1, 14, 20, 30]) & (F.col("unit_price") < 130) & (F.col("unit_price") >= 350))
)
