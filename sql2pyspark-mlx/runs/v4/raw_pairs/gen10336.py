from pyspark.sql import functions as F

eligible = (
    df_products
    .alias("s")
    .join(df_inventory.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.product_id").isin([2, 3])), "left_anti")
)

joined = (
    eligible
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.rating"), F.col("b.supplier_name"))
)

joined_products = (
    joined
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select("a.*", F.col("b.country"), F.col("b.city"))
)

result = (
    joined_products
    .cube(
        "supplier_id",
        F.date_trunc("year", "launch_date").alias("year_start"),
        "category",
    )
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.avg("rating").alias("avg_rating"),
    )
    .orderBy(F.col("avg_rating").desc_nulls_last())
)
