from pyspark.sql import functions as F

rollup_totals = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["city", "country", "brand"], ["city", "country"], ["city"]],
        F.col("p.city").alias("city"),
        F.col("p.country").alias("country"),
        F.col("s.brand").alias("brand"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.sum(F.col("s.stock_qty")).alias("total_stock_qty"),
        F.min(F.col("s.price")).alias("min_price"),
    )
)

result = (
    rollup_totals
    .select(
        "country",
        "brand",
        "rows_total",
        "total_stock_qty",
        "min_price",
        F.when(F.col("brand").rlike("^(Initech|Stark)$"), "GROUP_A").otherwise("CORE").alias("brand_zone"),
    )
)
