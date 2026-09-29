from pyspark.sql import functions as F

base = (
    df_products
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-10-23"))) & F.col("supplier_id").isNotNull() & F.col("supplier_id").isNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_contains("tags", "sale").alias("is_sale"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("stock_qty") >= 396, "LARGE").otherwise("SMALL").alias("stock_qty_tier"),
        "brand",
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.avg(F.when(F.col("category") == "Sports", F.col("cost"))).alias("sports_avg_cost"),
    )
)
