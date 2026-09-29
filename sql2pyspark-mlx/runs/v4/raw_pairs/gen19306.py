from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "channel").orderBy(F.col("amount").desc(), "id")

ranked = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(web|partner)$"), "PRIORITY").otherwise("OTHER_ZONE").alias("channel_zone"),
    )
    .orderBy(F.col("shipping_fee").desc())
)
