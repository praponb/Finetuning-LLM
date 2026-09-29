from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("store_id").isin([26, 28, 36, 38]))
    .groupBy(
        F.when(F.year("order_date") < 2024, "before_2024").when(F.year("order_date") == 2024, "2024").otherwise("after_2024").alias("order_cohort"),
    )
    .agg(
        F.count("*").alias("total_count"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
    )
    .filter(F.col("total_count") < 4)
    .select("order_cohort", "total_count", "channel_list")
    .orderBy(F.col("total_count").desc())
)
