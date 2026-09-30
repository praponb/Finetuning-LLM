from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("order_date").desc(), "id")

ranked = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .select(
        "id",
        "channel",
        "amount",
        "shipping_fee",
        "rn",
        F.when(F.col("channel").rlike("^(web|store)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
)
