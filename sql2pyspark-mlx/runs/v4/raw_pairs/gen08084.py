from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date").between("2023-06-14", "2025-08-01"))
    .select("cost")
    .subtract(df_orders.select(F.col("shipping_fee").alias("cost")))
)
