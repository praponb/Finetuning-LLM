from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 4]))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.round((F.col("units") * 1.1), 2).alias("units_scaled"),
    )
)

result = (
    valid
    .select("id", "product_id", "units_scaled", "revenue")
    .unpivot(
        ["id", "product_id"],
        ["units_scaled", "revenue"],
        "metric_name",
        "reading",
    )
    .filter(F.col("reading").isNotNull())
)
