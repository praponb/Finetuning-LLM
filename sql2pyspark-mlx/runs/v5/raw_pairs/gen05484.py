from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id")
w2 = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("price", "id")
w4 = Window.partitionBy("category", "product_id").orderBy("prev_quantity", "id")
w5 = Window.partitionBy("product_id").orderBy("price", "id")
w6 = Window.partitionBy("category").orderBy("next_price", "id")
w7 = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

cleaned = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-08-12"))) & F.col("quantity").isNotNull())
    .withColumn("quantity_whole", F.floor("quantity"))
)

with_dims = (
    cleaned
    .alias("s")
    .join(df_products.alias("b"), F.col("s.product_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.category"), F.col("b.price"))
)

latest = (
    with_dims
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    latest
    .filter(F.col("store_id").isin([1, 2, 4, 6]))
    .select(
        "id",
        "product_id",
        "last_restock",
        "quantity_whole",
        "category",
        "price",
        "rank_in_group",
        F.last("price", ignorenulls=True).over(w2).alias("last_known_price"),
        F.lead("price", 2).over(w_ordered).alias("next_price"),
        F.lag("quantity", 2).over(w_ordered).alias("prev_quantity"),
    )
)

metrics = (
    with_windows
    .withColumn("prev_last_known_price", F.lag("last_known_price", 1).over(w4))
    .withColumn("price_bucket", F.ntile(4).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("last_known_prev_quantity", F.last("prev_quantity", ignorenulls=True).over(w7))
)

with_dims_inventory = (
    metrics
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.product_name"), F.col("b.cost"))
)

wide = (
    with_dims_inventory
    .groupBy("product_id")
    .pivot("category", ["Sports", "Fashion"])
    .agg(F.avg("cost").alias("value_sum"))
)

result = (
    wide
    .join(quantity_by_product_id, on="product_id", how="left")
    .select("product_id", "Sports", "Fashion", F.col("sum_inventory_quantity"))
    .orderBy(F.col("sum_inventory_quantity").desc_nulls_last())
)
