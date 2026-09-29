from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & F.col("category").isin(["Electronics", "Fashion", "Books"]))
    .withColumn("stock_qty_per_price", F.round((F.col("stock_qty") / F.col("price")), 2))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

with_latest = (
    filtered
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

eligible = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.units") > 40), "left_anti")
)

with_dims = (
    eligible
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.country"), F.col("p.city"))
)

totals = (
    with_dims
    .groupingSets([["brand", "city"], ["brand"]], "brand", "city")
    .agg(
        F.percentile_approx("inventory_quantity", [0.25, 0.5, 0.75]).alias("inventory_quantity_quartiles"),
        F.round(F.sum("stock_qty_per_price") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("stock_qty_per_price_per_berlin"),
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.round(F.avg("price"), 2).alias("avg_price"),
        F.grouping("brand").alias("grouping_level"),
    )
    .filter(F.col("stock_qty_per_price_per_berlin") > 0)
)

result = (
    totals
    .select(
        "inventory_quantity_quartiles",
        "stock_qty_per_price_per_berlin",
        "avg_price",
    )
    .orderBy("avg_price")
)
