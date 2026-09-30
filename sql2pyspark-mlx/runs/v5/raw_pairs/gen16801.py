from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 12)
    .select("status")
    .unionByName(df_shipments.filter(F.col("carrier") == "UPS").select("status"))
    .distinct()
)
