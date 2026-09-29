from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy("channel")
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.max("amount").alias("max_amount"),
    )
    .filter(F.col("max_amount") > 910)
    .orderBy("max_amount", "channel")
    .limit(10)
)
