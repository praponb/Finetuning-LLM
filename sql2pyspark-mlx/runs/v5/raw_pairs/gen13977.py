from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "DE")
    .select(
        "id",
        "country",
        F.upper("supplier_name").alias("supplier_name_upper"),
        F.round((F.col("rating") * 1.15), 2).alias("adjusted_rating"),
    )
    .orderBy(F.col("id").desc())
)
