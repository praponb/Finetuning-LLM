from pyspark.sql import functions as F

filtered = (
    df_store_sales
    .filter((F.col("sale_date") >= F.lit("2024-04-30").cast("date")) & (F.col("units") < 32))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        (F.col("revenue") - F.col("units")).alias("revenue_per_unit"),
    )
)

result = (
    filtered
    .select(
        "id",
        "product_id",
        F.col("revenue_per_unit").alias("REVENUE_PER_UNIT"),
        F.col("revenue").alias("REVENUE_VAL"),
    )
    .unpivot(
        ["id", "product_id"],
        ["REVENUE_PER_UNIT", "REVENUE_VAL"],
        "metric",
        "value",
    )
)
