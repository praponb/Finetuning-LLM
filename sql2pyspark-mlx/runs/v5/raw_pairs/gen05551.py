from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("status").orderBy("amount", "id")
w2 = Window.partitionBy("customer_id")
w3 = Window.partitionBy("channel", "customer_id").orderBy("amount", "id").rowsBetween(-3, Window.currentRow)

enriched = (
    df_orders
    .withColumn("amount_bucket", F.ntile(10).over(w_recent))
    .withColumn("amount_share", F.round((F.col("amount") / F.sum("amount").over(w2)), 4))
    .withColumn("discount_avg_3", F.avg("discount").over(w3))
    .withColumn("corr_shipping_fee_discount", F.corr("shipping_fee", "discount").over(w2))
)

result = (
    enriched
    .select(
        "id",
        "order_date",
        "channel",
        "amount",
        "discount",
        "amount_bucket",
        "corr_shipping_fee_discount",
        F.when(F.col("channel").rlike("^(partner|store)$"), "OTHER_ZONE").when(F.col("channel").rlike("^(web|mobile)$"), "EXTENDED").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy("channel_zone")
)
