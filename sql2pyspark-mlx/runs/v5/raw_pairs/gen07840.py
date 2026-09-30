from pyspark.sql import functions as F

result = (
    df_orders
    .groupBy(
        F.when(F.col("status").isin(["COMPLETED", "SHIPPED"]), "core").otherwise("other").alias("status_group"),
        "channel",
    )
    .agg(F.countDistinct("amount").alias("unique_stores"))
    .orderBy(F.col("status_group").desc(), F.col("channel").desc())
)
