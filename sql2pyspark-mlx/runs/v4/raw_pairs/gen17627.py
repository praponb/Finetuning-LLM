from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("amount").between(1110, 1320))
    .groupBy("status")
    .agg(
        F.min("discount").alias("min_discount"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("amount").alias("unique_stores"),
    )
)
