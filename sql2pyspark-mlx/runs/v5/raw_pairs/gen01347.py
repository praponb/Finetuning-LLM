from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())

valid = (
    df_products
    .filter(F.col("cost").between(490, 520))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "launch_date",
        "tags",
        "supplier_id",
        F.add_months("launch_date", 1).alias("launch_date_plus_months"),
        F.when(F.col("category") == "Books", 1).otherwise(0).alias("is_books"),
    )
)

with_latest = (
    valid
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

valid_products = (
    with_latest
    .filter((~F.col("brand").eqNullSafe(F.lit("Globex"))) & (F.col("launch_date") >= F.to_date(F.lit("2023-06-15"))))
    .withColumn("price_minus_inventory_quantity", (F.col("price") - F.col("inventory_quantity")))
    .withColumn("launch_date_label", F.date_format("launch_date", "yyyy-MM"))
)

enriched = (
    valid_products
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.rating"))
)

with_latest_products = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.last_restock")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.last_restock").alias("inventory_last_restock"))
)

joined = (
    with_latest_products
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.country") == "UK")
    .select(
        "e.id",
        "e.product_name",
        "e.price",
        "e.launch_date",
        "e.supplier_id",
        "e.launch_date_plus_months",
        "e.last_restock",
        "e.price_minus_inventory_quantity",
        "e.launch_date_label",
        "e.rating",
        F.col("b.city"),
        F.col("b.rating").alias("supplier_rating"),
    )
)

enriched_products = (
    joined
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "left")
    .select(
        "a.id",
        "a.price",
        "a.launch_date",
        "a.supplier_id",
        "a.last_restock",
        "a.rating",
        "a.city",
        F.col("s.country"),
        F.col("s.rating").alias("supplier_rating_2"),
    )
)

result = (
    enriched_products
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["supplier_country", "supplier_city", "product_city"], ["supplier_country", "supplier_city"], ["supplier_country"], []],
        F.col("d.country").alias("supplier_country"),
        F.col("d.city").alias("supplier_city"),
        F.col("s.city").alias("product_city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.min(F.col("s.supplier_rating_2")).alias("min_supplier_rating_2"),
        F.grouping_id().alias("level_id"),
    )
)
