from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") == "DHL") & (F.col("carrier") == "Kerry"))
    .select("shipped_date", "status")
)
