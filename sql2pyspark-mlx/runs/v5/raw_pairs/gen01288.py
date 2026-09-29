from pyspark.sql import functions as F

eligible = (
    df_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([10, 15, 28])), "left_anti")
)

base = (
    eligible
    .filter(F.array_contains("tags", "bestseller"))
    .select(
        "*",
        F.when(F.col("category") == "Grocery", 1).otherwise(0).alias("is_grocery"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

cleaned = (
    base
    .filter(F.col("brand").isNotNull() & (F.col("is_grocery") > 56))
    .withColumn("is_grocery_times_cost", (F.col("is_grocery") * F.col("cost")))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity").between(3, 4)), "left_semi")
)

filtered = (
    not_flagged
    .filter(F.col("launch_date") >= F.lit("2023-05-07").cast("date"))
    .withColumn("cost_scaled", F.round((F.col("cost") * 1.15), 2))
)

result = (
    filtered
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "is_grocery",
    )
    .orderBy(F.col("cost").asc_nulls_last())
)
