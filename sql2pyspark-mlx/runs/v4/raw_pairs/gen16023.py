from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy("shipping_fee", "id")
shipping_fee_by_store_id = df_orders.groupBy("store_id").agg(F.sum("shipping_fee").alias("sum_order_shipping_fee"))

deduped = (
    df_orders
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

result = (
    deduped
    .join(shipping_fee_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "latest_rank",
        F.when(F.col("status").rlike("^(SHIPPED|COMPLETED)$"), "GROUP_A").when(F.col("status").rlike("^(PENDING|CANCELLED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
        F.col("sum_order_shipping_fee"),
    )
    .orderBy(F.col("discount").desc())
)
