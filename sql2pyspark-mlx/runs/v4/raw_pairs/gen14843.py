from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "DE") & (F.col("rating") > 2.9))
    .select(
        "id",
        "rating",
        "supplier_name",
        F.trim("city").alias("clean_city"),
        F.round((F.col("rating") * 1.1), 2).alias("adjusted_rating"),
    )
    .orderBy("supplier_name", "id")
    .limit(20)
)
