from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id", "store_id")
w3 = Window.partitionBy("store_id")
w4 = Window.partitionBy("product_id", "store_id").orderBy(F.col("quantity").desc())
w5 = Window.orderBy("quantity", "id")
w6 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("product_id").orderBy("seq_no", "id")
w8 = Window.partitionBy("product_id").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-04-28").cast("date"))
    .select("*", F.col("quantity").cast("string").alias("quantity_str"))
)

metrics = (
    valid
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("product_id_count_quantity", F.count("quantity").over(w2))
    .withColumn("store_id_count_quantity", F.count("quantity").over(w3))
    .withColumn("quantity_rank", F.rank().over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

filtered = (
    metrics
    .filter(F.col("seq_no").between(5, 7))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_str",
        "product_id_count_quantity",
        "store_id_count_quantity",
        "seq_no",
        F.when(F.col("quantity") >= 251, "High").when(F.col("quantity") >= 68, "Medium").otherwise("Low").alias("quantity_tier"),
        F.col("event_seq").cast("string").alias("event_seq_str"),
    )
)

metrics_inventory = (
    filtered
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_str",
        "store_id_count_quantity",
        "seq_no",
        "quantity_tier",
        "event_seq_str",
        F.first("store_id_count_quantity", ignorenulls=True).over(w6).alias("first_known_store_id_count_quantity"),
        F.lag("product_id_count_quantity", 1).over(w7).alias("prev_product_id_count_quantity"),
        F.sum("quantity").over(w8).alias("cumulative_quantity"),
    )
)

enriched = (
    metrics_inventory
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity_str",
        "e.store_id_count_quantity",
        "e.seq_no",
        "e.first_known_store_id_count_quantity",
        "e.prev_product_id_count_quantity",
        "e.cumulative_quantity",
        F.col("b.category"),
        F.col("b.brand"),
        F.col("b.product_name"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "new"))
    .select("e.*", F.col("p.brand").alias("product_brand"), F.col("p.cost"))
)

result = (
    with_dims
    .alias("s")
    .join(df_products.alias("dim"), F.col("s.product_id") == F.col("dim.id"), "inner")
    .groupBy(
        F.col("dim.brand").alias("product_brand"),
        F.col("dim.category").alias("product_category"),
        F.col("s.category").alias("inventory_category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("s.store_id_count_quantity")), 2).alias("total_store_id_count_quantity"),
    )
    .orderBy(F.col("total_store_id_count_quantity").asc_nulls_last())
)
