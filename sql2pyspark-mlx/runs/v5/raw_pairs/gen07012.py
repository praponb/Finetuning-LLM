from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.col("carrier").isin(["UPS", "FedEx"]) & F.col("cost").isNull())
    .select("status")
    .orderBy("status")
)
