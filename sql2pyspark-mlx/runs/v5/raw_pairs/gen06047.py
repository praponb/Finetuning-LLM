from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.amount").desc(), F.col("c.id").desc())

with_last = (
    df_customers
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.customer_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.name", "s.email", F.col("c.channel").alias("order_channel"))
)

result = (
    with_last
    .groupBy("order_channel")
    .agg(
        F.sort_array(F.collect_set("order_channel")).alias("order_channel_list"),
        F.count(F.when(F.col("order_channel") == "store", 1)).alias("store_count"),
        F.count("*").alias("cnt"),
        F.size(F.collect_set("order_channel")).alias("num_order_channels"),
        F.count("*").alias("cnt_2"),
        F.size(F.collect_set("order_channel")).alias("num_order_channels_2"),
    )
)
