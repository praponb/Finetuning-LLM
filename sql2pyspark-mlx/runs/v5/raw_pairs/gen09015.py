from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["RETURNED", "LOST", "DELIVERED"]) & (F.col("status") == "LOST"))
    .groupBy(F.date_trunc("MONTH", "delivered_date").alias("delivered_month"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.min("cost").alias("min_cost"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
    )
)
