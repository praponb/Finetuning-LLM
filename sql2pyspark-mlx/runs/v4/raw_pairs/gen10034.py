from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("quantity_whole", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity", "id")
w3 = Window.partitionBy("store_id").orderBy("quantity_whole", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc(), "id")

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-08-21").cast("date")) & (F.col("quantity") >= 231))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        F.ceil("quantity").alias("quantity_whole"),
    )
)

metrics = (
    cleaned
    .withColumn("last_known_quantity_whole", F.last("quantity_whole", ignorenulls=True).over(w))
    .withColumn("event_seq", F.row_number().over(w2))
)

ranked = (
    metrics
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
)

deduped = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w4))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    deduped
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.*",
        F.col("p.product_name"),
        F.col("p.launch_date"),
        F.col("p.category"),
    )
)

result = (
    enriched
    .groupBy("store_id", "product_id")
    .pivot("category", ["Grocery", "Books", "Fashion"])
    .agg(
        F.sum("last_known_quantity_whole").alias("value_sum"),
        F.count(F.lit(1)).alias("item_count"),
    )
    .select(
        "store_id",
        "product_id",
        F.col("Grocery_value_sum").alias("grocery_value_sum"),
        F.col("Grocery_item_count").alias("grocery_item_count"),
        F.col("Books_value_sum").alias("books_value_sum"),
        F.col("Books_item_count").alias("books_item_count"),
        F.col("Fashion_value_sum").alias("fashion_value_sum"),
        F.col("Fashion_item_count").alias("fashion_item_count"),
    )
)
