from pyspark.sql import functions as F

result = (
    df_order_items
    .filter(~((F.col("order_id") == 21) & (F.col("unit_price") > 240)))
    .select("id", "quantity")
)
