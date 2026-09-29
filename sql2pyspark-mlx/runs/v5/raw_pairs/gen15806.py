from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(F.col("unit_price").isNull() & (F.col("quantity") > 5) & (F.col("unit_price") < 220))
)
