from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc_nulls_last(), "id")

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
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
        F.when(F.col("channel").rlike("^(store|mobile)$"), "EXTENDED").when(F.col("channel").rlike("^(web|partner)$"), "GROUP_A").otherwise("CORE").alias("channel_zone"),
    )
)
