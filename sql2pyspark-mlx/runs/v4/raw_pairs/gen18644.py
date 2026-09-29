from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-11-01", "2025-01-01"))
    .select("order_date", "shipping_fee", "amount")
    .orderBy(F.col("shipping_fee").desc())
)
