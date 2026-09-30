from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-06-01", "2025-08-01") & (F.col("shipping_fee") > 23))
    .select("channel")
)
