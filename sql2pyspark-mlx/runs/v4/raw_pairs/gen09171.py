from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_times_quantity_filled_int").desc())
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w4 = Window.partitionBy("product_id").orderBy("quantity")
w5 = Window.partitionBy("product_id").orderBy("quantity", "id")
w6 = Window.partitionBy("product_id").orderBy(F.col("quantity_filled").desc_nulls_first())
w7 = Window.partitionBy("store_id").orderBy("next_quantity_times_quantity_filled_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("store_id").orderBy(F.col("quantity_times_quantity_filled_int_dense_rank").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy("quantity_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("quantity_times_quantity_filled", "id").rowsBetween(-5, -1)
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.min("quantity").alias("min_order_item_quantity"))

filtered = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-04-02")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.coalesce("quantity", F.lit(0)).alias("quantity_filled"),
    )
)

valid = (
    filtered
    .filter(F.col("last_restock") >= F.lit("2023-05-15").cast("date"))
    .select(
        "*",
        F.last_day("last_restock").alias("last_restock_month_end"),
        (F.col("quantity") * F.col("quantity_filled")).alias("quantity_times_quantity_filled"),
    )
)

base = (
    valid
    .filter(F.col("last_restock") > F.lit("2023-04-03").cast("date"))
    .withColumn("quantity_times_quantity_filled_int", F.col("quantity_times_quantity_filled").cast("int"))
)

metrics = (
    base
    .withColumn("next_quantity_times_quantity_filled_int", F.lead("quantity_times_quantity_filled_int", 1).over(w))
    .withColumn("quantity_times_quantity_filled_int_dense_rank", F.dense_rank().over(w2))
    .withColumn("quantity_bucket", F.ntile(5).over(w3))
    .withColumn("quantity_cume_dist", F.cume_dist().over(w4))
    .withColumn("quantity_bucket_2", F.ntile(5).over(w5))
)

with_windows = (
    metrics
    .withColumn("quantity_filled_dense_rank", F.dense_rank().over(w6))
    .withColumn("running_quantity", F.sum("quantity").over(w7))
    .withColumn("seq_no", F.row_number().over(w8))
    .withColumn("first_known_quantity_times_quantity_filled_int_dense_rank", F.first("quantity_times_quantity_filled_int_dense_rank", ignorenulls=True).over(w9))
    .withColumn("quantity_bucket_avg_5_prior", F.avg("quantity_bucket").over(w10))
)

result = (
    with_windows
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "last_restock_month_end",
        "quantity_bucket",
        "running_quantity",
        "seq_no",
        "quantity_bucket_avg_5_prior",
        F.col("min_order_item_quantity"),
    )
)
