from pyspark.sql import functions as F

result = (
    df_suppliers.alias("s")
    .join(df_products.alias("p"), F.col("p.supplier_id") == F.col("s.id"), "left")
    .groupBy(F.col("s.country"))
    .agg(
        F.count(F.col("p.id")).alias("product_count"),
        F.sum(F.when(F.col("p.brand") == "Globex", F.col("p.stock_qty")).otherwise(0)).alias("globex_stock_qty"),
        F.sum(F.col("p.stock_qty")).alias("total_stock_qty"),
    )
)
