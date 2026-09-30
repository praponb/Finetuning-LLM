from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "web"]), "group_a").otherwise("other").alias("channel_group"),
        "channel",
    )
    .agg(F.avg("amount").alias("avg_amount"))
    .filter(F.col("avg_amount") > 840)
    .select("channel_group", "channel", "avg_amount")
    .orderBy("channel")
)
