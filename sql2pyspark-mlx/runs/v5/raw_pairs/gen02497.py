from pyspark.sql import functions as F

result = (
    df_products
    .select(
        "id",
        "brand",
        F.when(F.col("category") == "Electronics", 1).otherwise(0).alias("is_electronics"),
    )
    .orderBy("is_electronics")
)
