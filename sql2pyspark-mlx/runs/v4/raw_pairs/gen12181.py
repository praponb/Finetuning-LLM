from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("status").isin(["IN_TRANSIT", "DELIVERED", "LOST"]))
    .groupBy("status")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("carrier"))).alias("carrier_values"),
        F.countDistinct("cost").alias("unique_orders"),
    )
    .orderBy("carrier_values", "unique_orders")
)
