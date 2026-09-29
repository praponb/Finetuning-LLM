from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("stock_qty")
w2 = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")
w3 = Window.partitionBy("category")
w4 = Window.partitionBy("supplier_id")

deduped = (
    df_products
    .withColumn("rank_in_group", F.rank().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

filtered = (
    deduped
    .filter(F.col("brand") != "Umbrella")
    .select(
        "*",
        F.greatest("stock_qty", "price").alias("greatest_stock_qty_price"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

enriched = (
    filtered
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "inner")
    .select("src.*", F.col("p.rating"))
)

deduped_products = (
    enriched
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
)

matched_keys = (
    deduped_products
    .select("brand", "category")
    .subtract(
        df_products
        .filter(~F.col("brand").isin(["Globex", "Stark", "Umbrella"]))
        .select("brand", "category")
    )
)

reconciled = (
    deduped_products
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .filter(F.col("supplier_id").isin([4, 29, 30, 31]))
    .withColumn("category_count_cost", F.count("cost").over(w3))
    .withColumn("supplier_id_sum_greatest_stock_qty_price", F.sum("greatest_stock_qty_price").over(w4))
)

result = (
    metrics
    .withColumn("year_start", F.date_trunc("YEAR", "launch_date"))
    .cube("year_start", "supplier_id")
    .agg(
        F.round(F.avg("supplier_id_sum_greatest_stock_qty_price"), 2).alias("avg_supplier_id_sum_greatest_stock_qty_price"),
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count("*").alias("cnt"),
        F.percentile_approx("price", [0.25, 0.5, 0.75]).alias("price_quartiles"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.grouping("year_start").alias("grouping_level"),
    )
)
