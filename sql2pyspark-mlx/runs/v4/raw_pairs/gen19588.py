from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("stock_qty").desc_nulls_last(), "id")
w2 = Window.partitionBy("brand").orderBy(F.col("stock_qty").asc_nulls_last(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

ranked = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

ranked_products = (
    ranked
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

not_flagged = (
    ranked_products
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date").between("2024-04-01", "2024-07-01")), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.quantity").alias("order_item_quantity"))
)

joined = (
    latest_match
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select("a.*", F.col("s.rating"), F.col("s.supplier_name"), F.col("s.country"))
)

enriched = (
    joined
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.supplier_name").alias("supplier_supplier_name"),
        F.col("d.country").alias("supplier_country"),
    )
)

remaining_keys = (
    enriched
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.size("tags") > 1)
        .select("supplier_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .select(
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "latest_rank",
    )
    .orderBy("stock_qty")
)
