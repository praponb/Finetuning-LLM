from pyspark.sql import functions as F

valid = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2024-03-01")))
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    valid
    .select("id", "store_id", "revenue_per_unit", "revenue")
    .unpivot(
        ["id", "store_id"],
        ["revenue_per_unit", "revenue"],
        "metric",
        "metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
