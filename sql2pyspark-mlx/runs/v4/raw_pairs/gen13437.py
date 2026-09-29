from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("shipping_fee", "id")
w2 = Window.partitionBy("store_id").orderBy(F.col("discount").desc_nulls_last())

enriched = (
    df_orders
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("discount_percent_rank", F.percent_rank().over(w2))
)

result = (
    enriched
    .groupBy(
        F.when(F.col("discount_percent_rank") >= 0.64, "PREMIUM").otherwise("STANDARD").alias("discount_percent_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.sum(F.when(F.col("channel") == "mobile", F.col("amount")).otherwise(0)).alias("mobile_amount"),
    )
    .filter(F.col("distinct_store_ids") < 1)
)
