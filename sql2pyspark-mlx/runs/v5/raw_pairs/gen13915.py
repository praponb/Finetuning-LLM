from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") <= "2024-06-28")
    .groupBy("carrier")
    .pivot("status", ["LOST", "IN_TRANSIT", "DELIVERED", "RETURNED"])
    .agg(F.avg("cost"))
    .orderBy("carrier")
)
