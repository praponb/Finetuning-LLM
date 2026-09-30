from pyspark.sql import functions as F

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("UNKNOWN")).alias("brand_clean"),
    )
)

result = (
    filtered
    .select("id", "brand", F.col("cost").alias("COST_VAL"), "price")
    .unpivot(
        ids=["id", "brand"],
        values=["COST_VAL", "price"],
        variableColumnName="metric_name",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
