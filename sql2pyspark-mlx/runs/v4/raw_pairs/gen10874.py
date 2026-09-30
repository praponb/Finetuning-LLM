from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy(F.col("shipping_fee").asc_nulls_first(), "id")

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

result = (
    ranked
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("channel") == "partner", 1)).alias("partner_count"),
        F.corr("amount", "shipping_fee").alias("corr_amount_shipping_fee"),
    )
    .orderBy(F.col("partner_count").asc_nulls_first())
)
