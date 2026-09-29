from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("price").isNull())
    .select("category")
    .unionByName(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("category")
    )
    .orderBy("category")
)
