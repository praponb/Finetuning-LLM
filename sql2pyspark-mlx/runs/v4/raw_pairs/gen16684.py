from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "store_id").orderBy(F.col("shipping_fee").asc_nulls_last(), "id")

ranked = (
    df_orders
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "channel",
        "amount",
        "discount",
        "shipping_fee",
        F.when(F.col("status").rlike("^(CANCELLED|RETURNED)$"), "PRIORITY").otherwise("EXTENDED").alias("status_zone"),
    )
    .orderBy("channel", "status")
)
