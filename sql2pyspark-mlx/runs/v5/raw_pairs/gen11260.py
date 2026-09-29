from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("store_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("product_id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.sum("quantity").alias("sum_inventory_quantity"))

enriched = (
    df_inventory
    .select(
        "id",
        "product_id",
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.max("quantity").over(w_running).alias("running_max_quantity"),
        F.min("quantity").over(w_running).alias("running_min_quantity"),
        F.count("quantity").over(w_running).alias("running_count_quantity"),
        F.max("quantity").over(w_recent).alias("product_id_max_quantity"),
    )
)

with_dims = (
    enriched
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.running_count_quantity",
        F.col("b.price"),
        F.col("b.category"),
        F.col("b.product_name"),
    )
)

remaining_keys = (
    with_dims
    .select("product_id", "id")
    .subtract(
        df_inventory
        .filter(F.col("product_id") == 4)
        .select("product_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

joined = (
    verified
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select(
        "e.*",
        F.col("p.category").alias("product_category"),
        F.col("p.brand"),
        F.col("p.stock_qty"),
    )
)

result = (
    joined
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "running_count_quantity",
        "price",
        "category",
        "product_name",
        "product_category",
        F.when(F.col("product_category").rlike("^(Fashion|Grocery)$"), "OTHER_ZONE").when(F.col("product_category").rlike("^(Electronics|Home)$"), "GROUP_A").otherwise("PRIORITY").alias("product_category_zone"),
        F.col("sum_inventory_quantity"),
    )
    .orderBy("sum_inventory_quantity")
)
