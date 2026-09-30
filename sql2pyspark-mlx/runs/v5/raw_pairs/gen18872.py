from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter((~F.col("city").eqNullSafe(F.lit("New York"))) & ~F.col("city").isin(["London", "Tokyo", "Bangkok"]))
    .withColumn("clean_supplier_name", F.trim("supplier_name"))
    .withColumn("rating_int", F.col("rating").cast("int"))
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (~F.col("c.product_name").like("Ba%")), "left_anti")
)

eligible_suppliers = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.stock_qty").between(200, 344)), "left_anti")
)

filtered_suppliers = (
    eligible_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "rating",
        "rating_int",
        F.length("city").alias("city_length"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("resolved_city"),
    )
)

eligible_suppliers_2 = (
    filtered_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= "2024-03-01"), "left_anti")
)

result = (
    eligible_suppliers_2
    .select("id", "city", "rating", "rating_int", "city_length", "resolved_city")
    .orderBy(F.col("city").desc_nulls_last())
)
