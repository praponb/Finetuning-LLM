from pyspark.sql import functions as F

result = (
    df_shipments
    .filter((F.col("delivered_date") >= "2025-08-19") & F.col("order_id").isin([16, 38]))
    .groupBy(
        F.when(F.col("cost") >= 87, "high").when(F.col("cost") >= 18, "mid").otherwise("low").alias("cost_bucket"),
    )
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
    )
)
