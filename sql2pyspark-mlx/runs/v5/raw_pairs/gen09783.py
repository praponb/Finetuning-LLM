from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.year("launch_date") == 2025) & F.col("supplier_id").isNotNull())
    .withColumn("stock_qty_whole", F.floor("stock_qty"))
)

remaining_keys = (
    filtered
    .select("supplier_id")
    .exceptAll(
        df_products
        .filter(F.col("cost") < 480)
        .select("supplier_id")
    )
)

verified = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

eligible = (
    verified
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date")), "left_semi")
)

matched_keys = (
    eligible
    .select("supplier_id", "id")
    .exceptAll(
        df_products
        .filter(F.col("brand") == "Acme")
        .select("supplier_id", "id")
    )
)

verified_products = (
    eligible
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.supplier_id") == F.col("k.supplier_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified_products
    .groupBy("category")
    .pivot("brand", ["Globex", "Acme", "Stark"])
    .agg(F.avg("cost").alias("total"), F.count(F.lit(1)).alias("cnt"))
)
