from pyspark.sql import functions as F

result = (
    df_order_items
    .select("unit_price")
    .union(df_payments.select("amount"))
    .distinct()
)
