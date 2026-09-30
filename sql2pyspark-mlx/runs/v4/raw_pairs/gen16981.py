from pyspark.sql import functions as F

filtered = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-09-12"))) & F.col("brand").isNotNull() & (F.col("price") > 930))
    .select(
        "id",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.least("cost", "stock_qty").alias("least_cost_stock_qty"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("cost") >= 630, "HIGH").otherwise("LOW").alias("cost_tier"),
        F.when(F.col("least_cost_stock_qty") >= 65, "PREMIUM").otherwise("STANDARD").alias("least_cost_stock_qty_tier"),
        F.date_trunc("MONTH", "launch_date").alias("activity_month"),
    )
    .agg(
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty"))).alias("acme_total_stock_qty"),
    )
    .filter(F.col("acme_total_stock_qty") < 0)
)
