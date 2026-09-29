from pyspark.sql import functions as F

base = df_products.filter((F.col("brand") != "Globex") & F.col("supplier_id").isNotNull())

cost_bucket_summary = (
    base
    .groupBy(
        F.when(F.col("cost") >= 560, "high").when(F.col("cost") >= 340, "mid").otherwise("low").alias("cost_bucket"),
        "category",
    )
    .agg(F.min("stock_qty").alias("min_stock_qty"))
)

result = (
    cost_bucket_summary
    .select("cost_bucket", "category", "min_stock_qty")
    .orderBy(F.col("min_stock_qty").desc(), "category", "cost_bucket")
    .limit(50)
)
