from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("category").isin(["Grocery", "Fashion", "Electronics"]) | (F.col("launch_date") >= "2025-10-05"))
    .select(
        "id",
        "product_name",
        "category",
        "cost",
        F.round((F.col("cost") / F.col("price")), 2).alias("margin"),
    )
    .orderBy(F.col("product_name").desc(), F.col("margin").desc())
)
