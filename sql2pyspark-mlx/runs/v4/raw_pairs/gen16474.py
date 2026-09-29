from pyspark.sql import functions as F

not_flagged = (
    df_products
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isNotNull()), "left_anti")
)

not_flagged_products = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date")), "left_anti")
)

enriched = (
    not_flagged_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.city"), F.col("b.supplier_name"))
)

base = (
    enriched
    .filter((F.col("launch_date") > F.lit("2023-10-23").cast("date")) & F.col("supplier_id").isNotNull() & F.col("product_name").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "city",
        "supplier_name",
        F.sort_array("tags").alias("sorted_tags"),
    )
)

result = (
    base
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "city",
        "supplier_name",
        F.when(F.col("supplier_name").rlike("^(Pacific Wholesale|Global Supply)$"), "CORE").otherwise("PRIORITY").alias("supplier_name_zone"),
    )
    .orderBy(F.col("city").desc())
)
