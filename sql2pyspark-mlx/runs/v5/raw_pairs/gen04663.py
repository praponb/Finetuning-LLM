from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_recent = Window.partitionBy("brand")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("rating").desc(), "id")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("brand_sum_price").desc(), "id")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

cleaned = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("brand").eqNullSafe(F.lit("Stark"))))
    .select("*", F.floor("price").alias("price_whole"))
)

joined = (
    cleaned
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_name",
        "a.category",
        "a.brand",
        "a.price",
        "a.stock_qty",
        "a.tags",
        "a.supplier_id",
        F.col("p.rating"),
        F.col("p.supplier_name"),
        F.col("p.city"),
    )
)

with_last = (
    joined
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.brand",
        "s.price",
        "s.tags",
        "s.supplier_id",
        "s.rating",
        F.col("c.last_restock").alias("inventory_last_restock"),
    )
)

not_flagged = (
    with_last
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([3, 8, 9, 10])), "left_anti")
)

enriched = (
    not_flagged
    .select(
        "*",
        F.sum("price").over(w_recent).alias("brand_sum_price"),
        F.round((F.col("price") / F.sum("price").over(w_recent)), 4).alias("price_share"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

latest_products = (
    latest
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
)

result = (
    latest_products
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "brand",
        "price",
        "tags",
        "supplier_id",
        "rating",
        "inventory_last_restock",
        "brand_sum_price",
        "price_share",
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("price_share").desc())
)
