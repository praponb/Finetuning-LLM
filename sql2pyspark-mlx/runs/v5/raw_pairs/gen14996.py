from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .intersect(df_payments.select("amount"))
    .orderBy("unit_price")
)
