from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("channel").isin(["mobile", "partner"]), "core").otherwise("other").alias("channel_group"),
    )
    .agg(
        (F.max("amount") - F.min("amount")).alias("amount_range"),
        F.max("discount").alias("max_discount"),
    )
)
