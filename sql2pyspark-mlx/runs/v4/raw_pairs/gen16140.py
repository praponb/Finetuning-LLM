from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("supplier_id", "category")
    .exceptAll(
        df_products
        .filter(F.year("launch_date") >= 2023)
        .select("supplier_id", "category")
    )
)

verified = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    verified
    .groupingSets(
        [["supplier_id", "category"], ["supplier_id"]],
        "supplier_id",
        "category",
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Globex", 1)), F.lit(0)), 2).alias("cost_per_globex"),
        F.countDistinct("category").alias("distinct_categories"),
        F.stddev_samp("stock_qty").alias("stock_qty_stddev"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
    )
    .filter(F.col("cost_per_globex") < 0)
)
