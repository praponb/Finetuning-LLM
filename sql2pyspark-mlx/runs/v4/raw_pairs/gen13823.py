from pyspark.sql import functions as F

result = (
    df_orders
    .filter(F.col("customer_id").isNotNull())
    .select(
        "id",
        "discount",
        F.when(F.col("channel").isin(["mobile", "partner"]), "Core").otherwise("Other").alias("channel_group"),
    )
    .orderBy("channel_group")
)
