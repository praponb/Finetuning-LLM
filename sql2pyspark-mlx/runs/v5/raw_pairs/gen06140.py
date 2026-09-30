from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("shipped_date") > "2024-08-01") & (F.col("delivered_date") < "2024-03-01"))
    .groupBy("status", "order_id")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("carrier") == "FedEx", 1).otherwise(0))) / F.count("*")), 1).alias("pct_fedex"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("cost")).otherwise(0)).alias("in_transit_cost"),
    )
    .filter(F.col("pct_fedex") >= 52)
)
