from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("amount").asc_nulls_first(), "id")
w2 = Window.partitionBy("customer_id").orderBy("amount", "id")
w3 = Window.orderBy("amount")
w4 = Window.partitionBy("customer_id").orderBy(F.col("prev_amount").desc(), "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.delivered_date").desc(), F.col("c.id").desc())

deduped = (
    df_orders
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "customer_id", "store_id", "amount", "shipping_fee")
)

metrics = (
    deduped
    .withColumn("prev_amount", F.lag("amount", 1).over(w2))
    .withColumn("amount_cume_dist", F.cume_dist().over(w3))
)

deduped_orders = (
    metrics
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

with_last = (
    deduped_orders
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn_2", F.row_number().over(w5))
    .filter(F.col("rn_2") == 1)
    .select("s.*", F.col("c.cost").alias("shipment_cost"))
)

result = (
    with_last
    .select(
        "id",
        "customer_id",
        "amount",
        "shipping_fee",
        "prev_amount",
        "amount_cume_dist",
        "rn",
    )
    .orderBy("rn", "amount")
)
