from pyspark.sql import functions as F

base = (
    df_shipments
    .filter((F.col("status").isin(["LOST", "DELIVERED"]) & F.col("carrier").isin(["FedEx", "UPS", "DHL"])) | (F.col("status") != "DELIVERED"))
)

shipped_month_summary = (
    base
    .groupBy(F.month("shipped_date").alias("shipped_month"))
    .agg(F.count("*").alias("num_shipments"))
)

result = (
    shipped_month_summary
    .filter(F.col("num_shipments") > 7)
    .orderBy("shipped_month")
)
