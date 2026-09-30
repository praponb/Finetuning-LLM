from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("supplier_id", "brand")
w3 = Window.partitionBy("brand", "city").orderBy("cost", "id").rowsBetween(-6, -1)
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.array_contains("tags", "eco"))
    .withColumn("category_prefix", F.substring("category", 1, 4))
    .withColumn("brand_clean", F.coalesce("brand", F.lit("N/A")))
)

joined = (
    cleaned
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.city"))
)

metrics = (
    joined
    .select(
        "*",
        F.sum("cost").over(w).alias("running_cost"),
        F.corr("price", "stock_qty").over(w2).alias("corr_price_stock_qty"),
        F.avg("stock_qty").over(w3).alias("stock_qty_avg_6_prior"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity").alias("inventory_quantity"),
    )
)

valid = (
    with_last
    .filter(F.col("supplier_id").isNotNull() & (F.col("stock_qty_avg_6_prior") >= 208))
    .withColumn("category_group", F.when(F.col("category").isin(["Fashion", "Grocery"]), "Core").otherwise("Other"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("Unknown")))
    .withColumn("brand_clean_2", F.coalesce("brand", F.lit("UNKNOWN")))
)

with_dims = (
    valid
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.city").alias("supplier_city"))
)

result = (
    with_dims
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "tags",
        "supplier_id",
        "city",
        "running_cost",
        F.when(F.col("category").rlike("^(Electronics|Fashion)$"), "OTHER_ZONE").otherwise("CORE").alias("category_zone"),
    )
    .orderBy(F.col("id").desc_nulls_first())
)
