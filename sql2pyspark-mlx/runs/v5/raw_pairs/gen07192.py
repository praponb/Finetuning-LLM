from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["DELIVERED", "LOST", "RETURNED"]) | (F.col("cost") > 87))
    .select("status", "carrier", "shipped_date")
)
