from pyspark.sql import functions as F

result = (
    df_order_items
    .select("id", "quantity", "unit_price")
    .orderBy("id")
)
