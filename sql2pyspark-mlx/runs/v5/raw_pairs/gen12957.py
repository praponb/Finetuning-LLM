from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("order_id") == 35) & ~F.col("carrier").isin(["UPS", "DHL"]) & F.col("order_id").isNotNull())
    .groupBy(
        F.when(F.col("cost") >= 80, "high").when(F.col("cost") >= 35, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sum(F.when(F.col("status") == "RETURNED", F.col("cost")).otherwise(0)).alias("returned_cost"),
    )
)
