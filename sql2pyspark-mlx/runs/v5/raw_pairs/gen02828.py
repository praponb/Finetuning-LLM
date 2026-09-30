from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "customer_id").orderBy(F.col("shipping_fee").desc(), "id")

latest = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    latest
    .select(
        "id",
        "channel",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(store|mobile)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
    .orderBy("channel")
)
