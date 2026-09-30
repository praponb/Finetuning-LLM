from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("status", F.year("order_date").alias("order_year"))
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.count("amount").alias("amount_count"),
    )
)
