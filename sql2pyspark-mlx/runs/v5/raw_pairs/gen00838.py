from pyspark.sql import functions as F

result = (
    df_order_items
    .filter((F.col("unit_price") < 370) | (F.col("unit_price") >= 320))
    .select("unit_price")
)
