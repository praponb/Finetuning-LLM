from pyspark.sql import functions as F

revenue_by_product_id = df_store_sales.groupBy("product_id").agg(F.sum("revenue").alias("sum_store_sale_revenue"))

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("quantity_whole", F.ceil("quantity"))
)

with_dims = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date").between("2024-07-06", "2024-08-18"))
    .select("e.*", F.col("p.cost"))
)

cleaned = (
    with_dims
    .filter(F.col("cost") > 260)
    .withColumn("quantity_whole_str", F.col("quantity_whole").cast("string"))
)

matched_keys = (
    cleaned
    .select("id", "product_id")
    .exceptAll(
        df_order_items
        .filter(F.col("order_id").isin([16, 22, 38]))
        .select("id", "product_id")
    )
)

verified = (
    cleaned
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.product_id") == F.col("k.product_id")), "inner")
    .select("s.*")
)

valid_order_items = (
    verified
    .filter(F.col("cost") >= 460)
    .select("*", (F.col("quantity") - F.col("unit_price")).alias("line_total"))
)

filtered = (
    valid_order_items
    .filter(F.col("product_id").isin([27, 32]))
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_whole",
        "cost",
        "quantity_whole_str",
        "line_total",
        F.round("quantity", 2).alias("quantity_rounded"),
        F.round((F.col("line_total") * 100), 2).alias("adjusted_line_total"),
    )
)

base = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .withColumn("adjusted_line_total_times_quantity_whole", (F.col("adjusted_line_total") * F.col("quantity_whole")))
    .withColumn("quantity_rounded_band", F.when(F.col("quantity_rounded") >= 77, "High").when(F.col("quantity_rounded") >= 38, "Medium").otherwise("Low"))
)

result = (
    base
    .join(revenue_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "quantity_whole",
        "cost",
        "quantity_whole_str",
        "quantity_rounded",
        "adjusted_line_total",
        F.col("sum_store_sale_revenue"),
    )
)
