from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status") == "LOST")
    .select("order_id", "carrier")
    .unionByName(df_shipments.filter(F.col("cost") <= 59).select("order_id", "carrier"))
    .distinct()
    .orderBy("carrier")
)
