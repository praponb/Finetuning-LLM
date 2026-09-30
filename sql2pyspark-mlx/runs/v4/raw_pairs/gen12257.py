from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount") > 1500)
    .groupBy("store_id")
    .agg(
        F.sum(F.when(F.col("status") == "CANCELLED", F.col("amount")).otherwise(0)).alias("cancelled_amount"),
        F.concat_ws(", ", F.sort_array(F.collect_set("channel"))).alias("channel_values"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("cancelled_amount") < 60)
    .orderBy("cancelled_amount")
)
