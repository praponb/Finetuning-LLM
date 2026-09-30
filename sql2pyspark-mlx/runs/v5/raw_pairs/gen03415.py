from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter(F.col("sale_date") > "2025-03-16")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") / F.col("revenue")), 2).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .select("id", "product_id", "revenue_per_unit", "revenue")
    .unpivot(
        ids=["id", "product_id"],
        values=["revenue_per_unit", "revenue"],
        variableColumnName="metric_name",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
