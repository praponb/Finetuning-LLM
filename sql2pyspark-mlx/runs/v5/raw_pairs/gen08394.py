from pyspark.sql import functions as F

result = (
    df_promotions
    .groupBy(F.date_trunc("MONTH", "starts_on").alias("activity_month"))
    .agg(
        F.count_if(F.col("channel") == "store").alias("store_events"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sort_array(F.collect_set("channel")).alias("channel_list_2"),
    )
    .filter(F.col("store_events") >= 1)
)
