from pyspark.sql import functions as F

units_by_product_id = df_store_sales.groupBy("product_id").agg(F.avg("units").alias("avg_store_sale_units"))

base = (
    df_order_items
    .filter(F.col("product_id").isNull())
    .withColumn("greatest_unit_price_quantity", F.greatest("unit_price", "quantity"))
    .withColumn("unit_price_int", F.col("unit_price").cast("int"))
)

joined = (
    base
    .alias("src")
    .join(df_orders.alias("o"), F.col("src.order_id") == F.col("o.id"), "inner")
    .select("src.*", F.col("o.shipping_fee"), F.col("o.status"), F.col("o.discount"))
)

filtered = (
    joined
    .filter(F.col("status").isNotNull() & (F.col("greatest_unit_price_quantity") > 56))
    .select(
        "*",
        (F.col("quantity") * F.col("discount")).alias("quantity_times_discount"),
        F.trim("status").alias("clean_status"),
    )
)

result = (
    filtered
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "greatest_unit_price_quantity",
        "unit_price_int",
        "status",
        "discount",
        "quantity_times_discount",
        F.col("avg_store_sale_units"),
    )
    .orderBy(F.col("avg_store_sale_units").desc())
)
