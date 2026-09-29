from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("channel").isNull())
    .groupBy(
        F.when(F.col("channel").isin(["store", "partner"]), "priority").otherwise("other").alias("channel_group"),
    )
    .agg(
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        (F.max("amount") - F.min("amount")).alias("amount_range"),
    )
    .filter(F.col("amount_range") > 56)
    .orderBy("channel_group")
)
