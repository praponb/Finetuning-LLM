from pyspark.sql import functions as F

result = (
    df_products
    .filter(F.col("supplier_id").isin([20, 35]))
    .select(
        "id",
        "launch_date",
        F.concat("category", F.lit(" - "), "brand").alias("category_brand"),
    )
    .orderBy(F.col("id").desc())
)
