from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-12-07"))) & F.col("supplier_id").isNotNull() & F.col("supplier_id").isNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.date_trunc("MONTH", "launch_date").alias("launch_date_period"),
        F.length("category").alias("category_length"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("resolved_brand"),
    )
)

result = (
    valid
    .groupBy("brand")
    .agg(
        F.max("launch_date_period").alias("last_seen"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("price")).otherwise(0)).alias("grocery_price"),
    )
)
