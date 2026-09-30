from pyspark.sql import functions as F

base = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-23").cast("date"))
    .select(
        "*",
        F.upper("category").alias("category_upper"),
        F.coalesce("brand", F.lit("Unknown")).alias("brand_filled"),
    )
)

result = (
    base
    .select("id", "brand", "cost", "price")
    .unpivot(
        ids=["id", "brand"],
        values=["cost", "price"],
        variableColumnName="kpi",
        valueColumnName="reading",
    )
)
