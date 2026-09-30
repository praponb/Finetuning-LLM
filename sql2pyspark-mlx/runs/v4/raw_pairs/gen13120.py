from pyspark.sql import functions as F

valid = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-12-17").cast("date")) & F.col("brand").isNotNull() & F.col("launch_date").between("2024-03-01", "2025-02-01"))
    .withColumn("cost_scaled", F.round((F.col("cost") * 1.07), 2))
    .withColumn("least_cost_price", F.least("cost", "price"))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("UNKNOWN")))
)

result = (
    valid
    .withColumn("stock_qty_tier", F.when(F.col("stock_qty") >= 382, "PREMIUM").otherwise("STANDARD"))
    .withColumn("week_start", F.date_trunc("WEEK", "launch_date"))
    .rollup("stock_qty_tier", "week_start", "supplier_id")
    .agg(
        F.max(F.when(F.col("category") == "Fashion", F.col("stock_qty"))).alias("fashion_max_stock_qty"),
        F.max(F.when(F.col("category") == "Books", F.col("price"))).alias("books_max_price"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("cost")).otherwise(0)).alias("acme_cost"),
        F.round(F.avg("least_cost_price"), 2).alias("avg_least_cost_price"),
        F.grouping("stock_qty_tier").alias("grouping_id"),
    )
    .filter(F.col("fashion_max_stock_qty") > 0)
    .orderBy(F.col("acme_cost").desc())
)
