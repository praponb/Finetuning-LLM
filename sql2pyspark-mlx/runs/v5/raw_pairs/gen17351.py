from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-06-24", "2025-06-16"))
    .select("id", "shipping_fee", "discount")
)
