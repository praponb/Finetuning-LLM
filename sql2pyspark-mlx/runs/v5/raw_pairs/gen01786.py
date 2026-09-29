from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sum(F.when(F.col("channel") == "mobile", 1).otherwise(0)).alias("mobile_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
)
