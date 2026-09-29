from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("shipped_date") < "2025-02-01")
    .groupBy(
        "carrier",
        F.date_trunc("MONTH", "delivered_date").alias("delivered_month"),
    )
    .agg(
        F.sum("cost").alias("total_cost"),
        F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0)).alias("fedex_count"),
    )
    .filter(F.col("fedex_count") >= 39)
    .drop("fedex_count")
)
