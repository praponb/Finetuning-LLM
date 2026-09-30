from pyspark.sql import functions as F

enriched = (
    df_products
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.supplier_name"), F.col("b.country"))
)

result = (
    enriched
    .rollup(
        F.date_trunc("QUARTER", "launch_date").alias("quarter_start"),
        F.when(F.col("price") >= 1220, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.corr("stock_qty", "cost").alias("corr_stock_qty_cost"),
    )
    .orderBy(F.col("num_categories").desc_nulls_last())
)
