from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("category", "brand").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id").orderBy(F.col("cost").desc_nulls_last(), "id")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("berlin_avg_cumulative_cost").desc())

metrics = (
    df_products
    .filter(F.col("supplier_id").isin([19, 23, 25]))
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        "stock_qty",
        "supplier_id",
        F.round((F.col("price") / F.sum("price").over(w)), 4).alias("price_share"),
        F.sum("cost").over(w2).alias("cumulative_cost"),
    )
)

with_dims = (
    metrics
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select(
        "s.id",
        "s.cost",
        "s.supplier_id",
        "s.price_share",
        "s.cumulative_cost",
        F.col("p.city"),
    )
)

not_flagged = (
    with_dims
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price") <= 230), "left_anti")
)

ranked = (
    not_flagged
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "supplier_id", "price_share", "cumulative_cost", "city")
)

kept = (
    ranked
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.store_id") == 2), "left_anti")
)

result = (
    kept
    .groupBy(
        "supplier_id",
        F.when(F.col("price_share") >= 0.76, "LARGE").otherwise("SMALL").alias("price_share_tier"),
    )
    .agg(
        F.round(F.avg("price_share"), 2).alias("avg_price_share"),
        F.percentile_approx("price_share", [0.25, 0.5, 0.75]).alias("price_share_quartiles"),
        F.count(F.when(F.col("city") == "Tokyo", 1)).alias("tokyo_count"),
        F.avg(F.when(F.col("city") == "Berlin", F.col("cumulative_cost"))).alias("berlin_avg_cumulative_cost"),
        F.max(F.when(F.col("city") == "Bangkok", F.col("cumulative_cost"))).alias("bangkok_max_cumulative_cost"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .withColumn("berlin_avg_cumulative_cost_dense_rank", F.dense_rank().over(w4))
    .orderBy("tokyo_count")
)
