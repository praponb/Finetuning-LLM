from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2025-04-09")
    .select("cost", "status")
    .orderBy(F.col("status").desc())
)
