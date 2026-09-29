from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy("last3_avg_discount", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("customer_id")
w4 = Window.orderBy(F.col("last3_avg_discount_int").desc(), "id")
w5 = Window.partitionBy("store_id", "status").orderBy("last3_avg_discount_int", "id")
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w_part = Window.orderBy("discount", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("channel", "status").orderBy("last3_count_discount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_part_2 = Window.partitionBy("channel").orderBy("last3_avg_discount_int_bucket", "id").rowsBetween(-5, -1)

with_windows = (
    df_orders
    .filter(F.year("order_date") == 2023)
    .withColumn("last3_count_discount", F.count("discount").over(w_ordered))
    .withColumn("last3_avg_discount", F.avg("discount").over(w_ordered))
    .withColumn("last3_min_discount", F.min("discount").over(w_ordered))
)

cleaned = (
    with_windows
    .filter(F.col("last3_avg_discount").between(0.24, 0.4))
    .withColumn("last3_avg_discount_int", F.col("last3_avg_discount").cast("int"))
)

scored = (
    cleaned
    .withColumn("last3_avg_discount_int_avg_6_prior", F.avg("last3_avg_discount_int").over(w2))
    .withColumn("customer_id_max_discount", F.max("discount").over(w3))
    .withColumn("last3_avg_discount_int_bucket", F.ntile(3).over(w4))
    .withColumn("last3_avg_discount_int_bucket_2", F.ntile(10).over(w5))
)

latest_match = (
    scored
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.store_id",
        "s.status",
        "s.channel",
        "s.discount",
        "s.last3_count_discount",
        "s.last3_avg_discount_int_avg_6_prior",
        "s.last3_avg_discount_int_bucket",
        F.col("c.quantity").alias("order_item_quantity"),
    )
)

metrics = (
    latest_match
    .select(
        "*",
        F.avg("order_item_quantity").over(w_part).alias("order_item_quantity_avg_5"),
        F.sum("order_item_quantity").over(w8).alias("running_order_item_quantity"),
        F.avg("order_item_quantity").over(w_part_2).alias("order_item_quantity_avg_5_prior"),
    )
)

result = (
    metrics
    .select(
        "id",
        "status",
        "last3_count_discount",
        "last3_avg_discount_int_avg_6_prior",
        "last3_avg_discount_int_bucket",
        "order_item_quantity_avg_5_prior",
    )
    .orderBy(F.col("last3_avg_discount_int_avg_6_prior").desc(), "id")
)
