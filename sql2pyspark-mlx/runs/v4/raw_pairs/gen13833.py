from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("id", "carrier", "delivered_date")
    .orderBy("id", "carrier")
    .limit(10)
)
