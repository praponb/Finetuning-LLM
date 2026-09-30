from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.round((F.col("rating") * 100), 2).alias("rating_adjusted"),
    )
    .orderBy("rating", "clean_supplier_name")
)
