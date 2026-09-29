from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isin([4, 35]) & F.col("channel").isin(["partner", "store"]) & (F.col("order_date") < "2023-03-01"))
    .groupBy("customer_id", "store_id")
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)
