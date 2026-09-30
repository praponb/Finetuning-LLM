from pyspark.sql import functions as F

result = (
    df_payments
    .select("amount")
    .union(df_order_items.select("unit_price"))
)
