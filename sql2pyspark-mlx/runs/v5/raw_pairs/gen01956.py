from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("order_date").desc())

deduped = (
    df_orders
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    deduped
    .select(
        "id",
        "store_id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "rn",
        F.when(F.col("channel").rlike("^(partner|web)$"), "PRIORITY").when(F.col("channel").rlike("^(mobile|store)$"), "OTHER_ZONE").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy("channel_zone", F.col("amount").desc())
)
