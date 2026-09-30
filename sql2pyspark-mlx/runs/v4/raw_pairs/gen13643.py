from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.array_contains("tags", "new"))
        .select("supplier_id", "id")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    kept_rows
    .cube(
        F.date_trunc("YEAR", "launch_date").alias("year_start"),
        F.when(F.col("cost") >= 560, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.avg(F.when(F.col("category") == "Electronics", F.col("stock_qty"))).alias("electronics_avg_stock_qty"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("cost_per_home"),
        F.count_if(F.col("brand") == "Stark").alias("stark_events"),
    )
)
