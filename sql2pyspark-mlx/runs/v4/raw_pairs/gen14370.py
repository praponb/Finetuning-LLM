from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-10-28").cast("date")) & (F.size("tags") > 2))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
)

remaining_keys = (
    filtered
    .select("supplier_id")
    .intersect(
        df_products
        .filter(F.col("launch_date") < "2023-11-01")
        .select("supplier_id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

with_dims = (
    reconciled
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country"), F.col("d.rating"), F.col("d.supplier_name"))
)

eligible = (
    with_dims
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= "2023-12-01"), "left_anti")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 4, 5])), "left_anti")
)

filtered_products = (
    not_flagged
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-11-19"))) & F.col("supplier_id").isNotNull() & (F.col("price") <= 1150))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "days_since_launch_date",
        "country",
        "rating",
        "supplier_name",
        F.length("supplier_name").alias("supplier_name_length"),
    )
)

not_flagged_products = (
    filtered_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNotNull()), "left_anti")
)

result = (
    not_flagged_products
    .groupBy(
        F.when(F.col("stock_qty") >= 442, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
    )
    .agg(
        F.bit_and("stock_qty").alias("stock_qty_bit_and"),
        F.corr("stock_qty", "price").alias("corr_stock_qty_price"),
        F.countDistinct("country").alias("distinct_countries"),
    )
)
