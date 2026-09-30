from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("carrier") != "Kerry") & (F.col("status") == "DELIVERED"))
    .select("delivered_date", "status")
)
