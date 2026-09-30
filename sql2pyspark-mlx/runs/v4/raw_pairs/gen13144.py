from pyspark.sql import functions as F

result = (
    df_products
    .select("supplier_id", "category")
    .subtract(
        df_products
        .filter(F.col("category").isin(["Home", "Books"]))
        .select("supplier_id", "category")
    )
)
