from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("supplier_name").isNotNull() & (F.col("rating") < 2.3)) | (F.col("city") == "New York"))
    .select(
        "id",
        "country",
        "rating",
        F.concat("supplier_name", F.lit("/"), "country").alias("supplier_name_country"),
    )
)
