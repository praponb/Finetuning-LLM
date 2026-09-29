from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("country") == "DE") & (F.col("supplier_name").like("%Sia%") | F.col("supplier_name").isNotNull()))
    .select(
        "id",
        "city",
        F.concat("city", F.lit("/"), "supplier_name").alias("city_supplier_name"),
        F.when(F.col("country") == "SG", 1).otherwise(0).alias("is_sg"),
    )
)
