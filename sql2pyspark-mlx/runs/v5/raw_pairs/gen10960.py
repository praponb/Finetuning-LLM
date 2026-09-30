from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.orderBy("quantity", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id")
w_recent = Window.partitionBy("store_id", "product_id").orderBy("product_id_min_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("quantity").desc(), "id")
w6 = Window.partitionBy("store_id", "product_id").orderBy("prev_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("prev_quantity", "id").rowsBetween(-5, Window.currentRow)
w8 = Window.partitionBy("store_id")
w9 = Window.partitionBy("product_id").orderBy("event_seq", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

enriched = (
    df_inventory
    .filter(F.col("last_restock") > "2023-06-19")
    .select(
        "*",
        F.lag("quantity", 1).over(w_grp).alias("prev_quantity"),
        F.last("quantity", ignorenulls=True).over(w2).alias("last_known_quantity"),
        F.min("quantity").over(w3).alias("product_id_min_quantity"),
    )
)

with_windows = (
    enriched
    .withColumn("running_quantity", F.sum("quantity").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w5))
)

scored = (
    with_windows
    .withColumn("running_min_event_seq", F.min("event_seq").over(w6))
    .withColumn("running_avg_event_seq", F.avg("event_seq").over(w6))
    .withColumn("running_max_event_seq", F.max("event_seq").over(w6))
)

enriched_inventory = (
    scored
    .select(
        "*",
        F.avg("quantity").over(w7).alias("quantity_avg_5"),
        F.stddev_samp("quantity").over(w8).alias("store_id_stddev_samp_quantity"),
        F.ntile(5).over(w9).alias("event_seq_bucket"),
        F.last("product_id_min_quantity", ignorenulls=True).over(w6).alias("last_known_product_id_min_quantity"),
    )
)

result = (
    enriched_inventory
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        "prev_quantity",
        "last_known_quantity",
        "product_id_min_quantity",
        "running_quantity",
        "event_seq",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("store_id").desc_nulls_first(), "product_id")
)
