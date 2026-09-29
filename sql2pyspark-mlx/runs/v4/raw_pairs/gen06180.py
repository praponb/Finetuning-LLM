from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.orderBy("unit_price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy("greatest_unit_price_quantity", "id")
w3 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.orderBy("unit_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("order_id")
w6 = Window.partitionBy("order_id", "product_id").orderBy(F.col("running_greatest_unit_price_quantity").desc())
w7 = Window.orderBy(F.col("last_known_unit_price").desc())
w8 = Window.partitionBy("product_id").orderBy(F.col("last_known_unit_price").desc(), "id")
quantity_by_order_id = df_order_items.groupBy("order_id").agg(F.max("quantity").alias("max_order_item_quantity"))

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("greatest_quantity_unit_price", F.greatest("quantity", "unit_price"))
)

base = (
    filtered
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 290))
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
)

base_order_items = (
    base
    .filter(F.col("quantity") != 8)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        "greatest_quantity_unit_price",
        "greatest_unit_price_quantity",
        (F.col("quantity") + F.col("greatest_unit_price_quantity")).alias("quantity_plus_greatest_unit_price_quantity"),
    )
)

with_windows = (
    base_order_items
    .withColumn("greatest_quantity_unit_price_avg_3", F.avg("greatest_quantity_unit_price").over(w_ordered))
    .withColumn("seq_no", F.row_number().over(w2))
    .withColumn("running_greatest_unit_price_quantity", F.sum("greatest_unit_price_quantity").over(w3))
    .withColumn("last_known_unit_price", F.last("unit_price", ignorenulls=True).over(w_hist))
    .withColumn("order_id_max_quantity", F.max("quantity").over(w_recent))
)

ranked = (
    with_windows
    .withColumn("rank_in_group", F.rank().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .withColumn("last_known_unit_price_rank", F.rank().over(w7))
    .withColumn("last_known_unit_price_bucket", F.ntile(4).over(w8))
)

result = (
    metrics
    .join(quantity_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_plus_greatest_unit_price_quantity",
        "order_id_max_quantity",
        "last_known_unit_price_bucket",
        F.col("max_order_item_quantity"),
    )
    .orderBy("unit_price")
)
