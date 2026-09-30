from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNull() | (F.col("status") == "DELIVERED"))
    .select("status", "shipped_date")
    .orderBy(F.col("status").desc())
)
