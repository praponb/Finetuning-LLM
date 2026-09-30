from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("product_id").isin([1, 3, 4, 10]))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.add_months("sale_date", 6).alias("sale_date_plus_months"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("units") >= 37, "LARGE").otherwise("SMALL").alias("units_tier"),
        "product_id",
    )
    .agg(
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
)
