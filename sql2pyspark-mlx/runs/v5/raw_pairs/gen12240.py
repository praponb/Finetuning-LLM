from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("brand").isin(["Stark", "Acme"]))
    .select("product_name", "category")
    .subtract(
        df_products
        .filter(F.col("cost") >= 620)
        .select("product_name", "category")
    )
    .orderBy("product_name")
)
