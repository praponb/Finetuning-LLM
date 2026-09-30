from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")

with_dims = (
    df_inventory
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select(
        "e.id",
        "e.product_id",
        "e.quantity",
        "e.last_restock",
        F.col("p.category"),
        F.col("p.cost"),
    )
)

remaining_keys = (
    with_dims
    .select("product_id", "id")
    .intersectAll(
        df_inventory
        .filter(F.col("quantity") <= 122)
        .select("product_id", "id")
    )
)

verified = (
    with_dims
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.product_id") == F.col("k.product_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

with_windows = verified.select("*", F.sum("quantity").over(w).alias("category_sum_quantity"))

with_dims_inventory = (
    with_windows
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.category",
        "a.cost",
        F.col("p.cost").alias("product_cost"),
    )
)

with_dims_inventory_2 = (
    with_dims_inventory
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.supplier_id").isNull())
    .select(
        "src.id",
        "src.product_id",
        "src.product_cost",
        F.col("p.stock_qty"),
        F.col("p.category").alias("product_category"),
    )
)

result = (
    with_dims_inventory_2
    .select("id", "product_id", "product_cost")
    .orderBy(F.col("product_cost").desc())
)
