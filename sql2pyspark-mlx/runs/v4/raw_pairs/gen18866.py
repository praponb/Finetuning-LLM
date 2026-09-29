from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isin(["UPS", "Kerry", "Flash"]))
    .select("status", "delivered_date")
    .unionByName(
        df_shipments
        .filter(F.col("carrier") == "Flash")
        .select("status", "delivered_date")
    )
    .distinct()
)
