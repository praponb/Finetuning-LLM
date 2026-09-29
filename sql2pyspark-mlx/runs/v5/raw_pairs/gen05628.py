from pyspark.sql import functions as F

matched_keys = (
    df_products
    .select("brand", "category")
    .exceptAll(
        df_products
        .filter(F.col("brand").isin(["Stark", "Initech", "Umbrella"]))
        .select("brand", "category")
    )
)

reconciled = (
    df_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("price_tier", F.when(F.col("price") >= 1250, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["price_tier", "category"], ["price_tier"]],
        "price_tier",
        "category",
    )
    .agg(
        F.count(F.when(F.col("brand") == "Stark", 1)).alias("stark_rows"),
        F.count("*").alias("row_count"),
        F.count(F.when(F.col("brand") == "Initech", 1)).alias("initech_count"),
        F.sum(F.when(F.col("category") == "Electronics", F.col("stock_qty")).otherwise(0)).alias("electronics_stock_qty"),
        F.avg(F.when(F.col("category") == "Fashion", F.col("stock_qty"))).alias("fashion_avg_stock_qty"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
    )
    .orderBy("electronics_stock_qty")
)
