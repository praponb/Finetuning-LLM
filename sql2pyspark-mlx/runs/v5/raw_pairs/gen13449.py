from pyspark.sql import functions as F

result = (
    df_orders
    .filter((F.col("status") != "COMPLETED") & F.col("discount").between(0.27, 0.36))
    .select("id", "store_id")
)
