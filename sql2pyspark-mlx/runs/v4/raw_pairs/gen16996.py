from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("brand").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2023-12-06")))
    .withColumn("category_group", F.when(F.col("category").isin(["Home", "Sports"]), "Group A").otherwise("Other"))
    .withColumn("price_rounded", F.round("price", 1))
)

metrics = (
    filtered
    .withColumn("corr_stock_qty_cost", F.corr("stock_qty", "cost").over(w))
    .withColumn("last_known_price_rounded", F.last("price_rounded", ignorenulls=True).over(w2))
)

not_flagged = (
    metrics
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.quantity") >= 5), "left_anti")
)

enriched = (
    not_flagged
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.city"), F.col("p.rating"))
)

result = (
    enriched
    .groupBy(F.when(F.col("cost") >= 720, "HIGH").otherwise("LOW").alias("cost_tier"))
    .agg(
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
        F.count("*").alias("num_rows"),
    )
    .orderBy(F.col("corr_stock_qty_cost").desc_nulls_last())
)
