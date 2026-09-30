from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("amount", "id")

latest = (
    df_orders
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

eligible = (
    latest
    .alias("s")
    .join(df_payments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.paid_at") >= F.col("s.order_date")), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.order_id").isin([11, 20, 25])), "left_anti")
)

result = (
    not_flagged
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("channel").rlike("^(partner|store)$"), "CORE").when(F.col("channel").rlike("^(web|mobile)$"), "GROUP_A").otherwise("GROUP_B").alias("channel_zone"),
    )
    .orderBy(F.col("status").desc_nulls_first())
)
