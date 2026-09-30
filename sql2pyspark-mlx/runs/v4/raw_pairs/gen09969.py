from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("s.id") == F.col("p.supplier_id"), "left")
    .filter(F.col("s.country") == "JP")
    .groupBy(F.col("s.id"), F.col("s.city"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.max(F.col("p.stock_qty")).alias("max_stock_qty"),
        F.round(((100 * F.sum(F.when(F.col("p.brand") == "Umbrella", 1).otherwise(0))) / F.count("*")), 1).alias("pct_umbrella"),
    )
    .filter(F.col("product_count") > 3)
    .orderBy(F.col("product_count").desc(), "city")
)
