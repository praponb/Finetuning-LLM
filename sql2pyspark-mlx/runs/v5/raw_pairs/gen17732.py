from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price") >= 630)
    .select("id", "product_name")
    .unionByName(
        df_products
        .filter(F.col("supplier_id").isin([15, 28, 38, 39]))
        .select("id", "product_name")
    )
)
