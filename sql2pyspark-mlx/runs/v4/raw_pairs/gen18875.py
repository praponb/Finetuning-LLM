from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id", "supplier_country").orderBy("cost", "id").rowsBetween(-3, -1)
w2 = Window.partitionBy("supplier_country").orderBy("launch_date", "id")
w3 = Window.partitionBy("supplier_id").orderBy("stock_qty_rounded", "id").rowsBetween(-5, Window.currentRow)
w4 = Window.partitionBy("country", "supplier_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-05-19"))) & F.col("supplier_id").isNotNull() & F.month("launch_date").isin([1, 5, 11]))
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.round("stock_qty", 2).alias("stock_qty_rounded"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

joined = (
    cleaned
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.country"), F.col("b.supplier_name"), F.col("b.rating"))
)

enriched = (
    joined
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .select(
        "e.*",
        F.col("s.country").alias("supplier_country"),
        F.col("s.supplier_name").alias("supplier_supplier_name"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.avg("cost").over(w).alias("cost_avg_3_prior"),
        F.lag("rating", 1).over(w2).alias("prev_rating"),
        F.avg("cost").over(w3).alias("cost_avg_5"),
        F.first("stock_qty_rounded", ignorenulls=True).over(w4).alias("first_known_stock_qty_rounded"),
    )
)

result = (
    metrics
    .groupBy(
        "supplier_id",
        F.when(F.col("rating") >= 4.1, "HIGH").otherwise("LOW").alias("rating_tier"),
    )
    .agg(
        F.percentile_approx("rating", [0.25, 0.5, 0.75]).alias("rating_quartiles"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.count(F.when(F.col("supplier_country") == "SG", 1)).alias("sg_rows"),
    )
    .orderBy(F.col("sg_rows").desc())
)
