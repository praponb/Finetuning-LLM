from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy("order_date", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.shipped_date").desc(), F.col("c.id").desc())

latest = (
    df_orders
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

with_latest = (
    latest
    .alias("s")
    .join(df_shipments.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.status").alias("shipment_status"),
        F.col("c.cost").alias("shipment_cost"),
    )
)

cleaned = (
    with_latest
    .filter(F.col("order_date") >= F.lit("2023-08-14").cast("date"))
    .select(
        "id",
        "customer_id",
        "store_id",
        "status",
        "amount",
        "discount",
        "shipping_fee",
        "dedup_rank",
        "shipment_status",
        "shipment_cost",
        (F.col("shipping_fee") + F.col("amount")).alias("amount_with_shipping"),
    )
)

result = (
    cleaned
    .groupBy("customer_id")
    .agg(
        F.count(F.when(F.col("shipment_status") == "DELIVERED", 1)).alias("delivered_rows"),
        F.count(F.when(F.col("shipment_status") == "DELIVERED", 1)).alias("delivered_count"),
        F.sum(F.when(F.col("status") == "COMPLETED", F.col("discount")).otherwise(0)).alias("completed_discount"),
    )
)
