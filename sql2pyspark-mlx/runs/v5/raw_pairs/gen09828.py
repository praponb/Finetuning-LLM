from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-04-30"))) & (F.col("cost") < 660))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "tags",
        "supplier_id",
        F.least("stock_qty", "cost").alias("least_stock_qty_cost"),
        F.sort_array("tags").alias("sorted_tags"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "left")
    .select(
        "e.id",
        "e.product_name",
        "e.tags",
        "e.supplier_id",
        "e.sorted_tags",
        F.col("p.city"),
    )
)

joined = (
    with_dims
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating").between(2.7, 3.6))
    .select(
        "e.id",
        "e.supplier_id",
        "e.sorted_tags",
        "e.city",
        F.col("s.country"),
        F.col("s.city").alias("supplier_city"),
    )
)

with_dims_products = (
    joined
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "inner")
    .select(
        "e.id",
        "e.supplier_id",
        "e.supplier_city",
        F.col("s.city").alias("supplier_city_2"),
        F.col("s.country").alias("supplier_country"),
        F.col("s.supplier_name"),
    )
)

enriched = (
    with_dims_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.supplier_id",
        "src.supplier_city",
        "src.supplier_city_2",
        F.col("b.rating"),
        F.col("b.country"),
    )
)

not_flagged = (
    enriched
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.quantity") > 5), "left_anti")
)

result = (
    not_flagged
    .alias("src")
    .join(df_suppliers.alias("dim"), F.col("src.supplier_id") == F.col("dim.id"), "inner")
    .groupingSets(
        [["city", "product_country"], ["city"], []],
        F.col("dim.city").alias("city"),
        F.col("src.country").alias("product_country"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.avg(F.col("src.rating")).alias("avg_rating"),
    )
    .orderBy(F.col("city").asc_nulls_first())
)
