from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("order_date").between("2024-07-01", "2025-06-28"))
    .select("id", "order_date", F.initcap(F.trim("channel")).alias("channel_title"))
    .orderBy("channel_title")
)
