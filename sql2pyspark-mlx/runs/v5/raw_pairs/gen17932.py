from pyspark.sql import functions as F

cleaned = (
    df_shipments
    .filter(F.col("cost") < 64)
    .select(
        "*",
        F.concat_ws(",", "status", "carrier").alias("status_carrier_key"),
        F.date_sub("delivered_date", 72).alias("delivered_date_minus_72d"),
        F.coalesce("carrier", F.lit("N/A")).alias("carrier_clean"),
    )
)

result = (
    cleaned
    .groupBy(F.date_trunc("quarter", "shipped_date").alias("quarter_start"))
    .agg(
        F.max(F.when(F.col("status") == "RETURNED", F.col("cost"))).alias("returned_max_cost"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
    )
)
