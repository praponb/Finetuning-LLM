from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status")
    .agg(
        F.countDistinct("discount").alias("unique_channels"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("unique_channels") > 7)
    .select("status", "unique_channels", "channel_list")
)
