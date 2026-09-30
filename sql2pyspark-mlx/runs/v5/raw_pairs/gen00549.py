from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("quantity_rounded").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity_rounded").desc_nulls_last())
w3 = Window.partitionBy("product_id").orderBy("quantity_rounded_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("store_id").orderBy("quantity_rounded_dense_rank")
w5 = Window.partitionBy("product_id").orderBy("running_max_quantity_rounded_dense_rank", "id").rowsBetween(-2, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("quantity").between(88, 124))
    .withColumn("quantity_rounded", F.round("quantity", 1))
)

enriched = (
    valid
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.ntile(4).over(w).alias("quantity_rounded_bucket"),
        F.dense_rank().over(w2).alias("quantity_rounded_dense_rank"),
    )
)

enriched_inventory = (
    enriched
    .filter(F.col("product_id") == 8)
    .withColumn("running_min_quantity_rounded_dense_rank", F.min("quantity_rounded_dense_rank").over(w3))
    .withColumn("running_max_quantity_rounded_dense_rank", F.max("quantity_rounded_dense_rank").over(w3))
    .withColumn("running_count_quantity_rounded_dense_rank", F.count("quantity_rounded_dense_rank").over(w3))
    .withColumn("quantity_rounded_dense_rank_percent_rank", F.percent_rank().over(w4))
)

enriched_inventory_2 = (
    enriched_inventory
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select(
        "s.id",
        "s.store_id",
        "s.product_id",
        "s.running_min_quantity_rounded_dense_rank",
        "s.running_max_quantity_rounded_dense_rank",
        "s.running_count_quantity_rounded_dense_rank",
        "s.quantity_rounded_dense_rank_percent_rank",
        F.col("p.product_name"),
        F.col("p.stock_qty"),
        F.col("p.launch_date"),
    )
)

scored = (
    enriched_inventory_2
    .select(
        "*",
        F.count("running_min_quantity_rounded_dense_rank").over(w5).alias("last3_count_running_min_quantity_rounded_dense_rank"),
        F.min("running_min_quantity_rounded_dense_rank").over(w5).alias("last3_min_running_min_quantity_rounded_dense_rank"),
        F.avg("running_min_quantity_rounded_dense_rank").over(w5).alias("last3_avg_running_min_quantity_rounded_dense_rank"),
    )
)

result = (
    scored
    .select(
        "id",
        "store_id",
        "product_id",
        "running_min_quantity_rounded_dense_rank",
        "running_max_quantity_rounded_dense_rank",
        "quantity_rounded_dense_rank_percent_rank",
        "product_name",
        "stock_qty",
        "last3_min_running_min_quantity_rounded_dense_rank",
        F.when(F.col("product_name").rlike("^(Desk Lamp|Running Shoes)$"), "GROUP_B").when(F.col("product_name").rlike("^(Laptop Pro 14|wireless mouse)$"), "CORE").otherwise("PRIORITY").alias("product_name_zone"),
    )
)
