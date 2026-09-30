from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")

filtered = (
    df_products
    .filter(F.col("price") <= 630)
    .select(
        "*",
        F.coalesce("cost", F.lit(0)).alias("cost_filled"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id").eqNullSafe(F.col("p.id")), "inner")
    .select("src.*", F.col("p.rating"), F.col("p.country"))
)

latest = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
)

remaining_keys = (
    latest
    .select("supplier_id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "eco"))
        .select("supplier_id")
    )
)

kept_rows = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") >= F.col("s.launch_date")), "left_anti")
)

eligible = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") > "2024-12-01"), "left_anti")
)

base = (
    eligible
    .filter(F.col("country").isNotNull())
    .select("*", F.array_join("tags", "|").alias("tags_text"))
)

result = (
    base
    .alias("a")
    .join(df_suppliers.alias("s"), F.col("a.supplier_id") == F.col("s.id"), "inner")
    .groupingSets(
        [["supplier_country", "city"], ["supplier_country"]],
        F.col("s.country").alias("supplier_country"),
        F.col("s.city").alias("city"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.avg(F.col("a.price")), 2).alias("avg_price"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("avg_price").asc_nulls_last())
)
