from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.order_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("order_status").orderBy(F.col("opened_date").desc_nulls_first(), "id")

filtered = (
    df_stores
    .filter(F.col("region").isNotNull() & (F.col("region") == "Central"))
    .select("*", F.quarter("opened_date").alias("opened_date_quarter"))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.opened_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.order_date").alias("order_order_date"),
        F.col("c.status").alias("order_status"),
    )
)

with_latest_stores = (
    with_latest
    .alias("s")
    .join(df_orders.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.order_date") <= F.col("s.order_order_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.shipping_fee"), F.col("c.discount"))
)

deduped = (
    with_latest_stores
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

eligible = (
    deduped
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.order_order_date")), "left_semi")
)

by_type = (
    eligible
    .groupBy("city")
    .pivot("order_status", ["CANCELLED", "RETURNED"])
    .agg(F.avg("discount").alias("amount"), F.count(F.lit(1)).alias("n"))
    .select(
        "city",
        F.col("CANCELLED_amount").alias("cancelled_amount"),
        F.col("CANCELLED_n").alias("cancelled_n"),
        F.col("RETURNED_amount").alias("returned_amount"),
        F.col("RETURNED_n").alias("returned_n"),
    )
)

result = (
    by_type
    .select("city", "cancelled_n", "returned_amount")
    .orderBy(F.col("returned_amount").desc())
)
