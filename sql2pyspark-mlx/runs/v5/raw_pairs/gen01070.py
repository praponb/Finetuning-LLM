from pyspark.sql import functions as F

valid = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .withColumn("category_group", F.when(F.col("category").isin(["Sports", "Books"]), "Core").otherwise("Other"))
)

result = (
    valid
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .groupBy(F.col("p.city").alias("city"), F.col("s.category").alias("category"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("s.stock_qty")).alias("min_stock_qty"),
        F.avg(F.col("s.price")).alias("avg_price"),
    )
)
