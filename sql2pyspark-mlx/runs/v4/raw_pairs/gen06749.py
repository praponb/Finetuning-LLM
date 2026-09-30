from pyspark.sql import functions as F

remaining_keys = (
    df_products
    .select("supplier_id", "id")
    .subtract(
        df_products
        .filter(F.col("supplier_id").isin([1, 4]))
        .select("supplier_id", "id")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .groupBy("brand", F.date_trunc("MONTH", "launch_date").alias("activity_month"))
    .agg(
        F.count("*").alias("row_count"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.count(F.when(F.col("brand") == "Umbrella", 1)).alias("umbrella_rows"),
        F.max(F.when(F.col("brand") == "Stark", F.col("cost"))).alias("stark_max_cost"),
    )
)
