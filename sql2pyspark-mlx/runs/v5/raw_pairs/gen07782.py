from pyspark.sql import functions as F

result = (
    df_payments
    .filter(F.col("paid_at") < "2025-08-01")
    .select("amount")
    .union(df_order_items.filter(F.col("quantity") == 3).select("unit_price"))
    .distinct()
)
