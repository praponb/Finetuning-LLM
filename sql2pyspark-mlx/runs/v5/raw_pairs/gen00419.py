from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id") == 7)
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.count("cost").alias("cost_count"),
        F.min("cost").alias("min_cost"),
    )
)
