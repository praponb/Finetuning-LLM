from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2023-04-18", "2025-08-01"))
    .select("channel")
    .distinct()
    .orderBy("channel")
    .limit(3)
)
