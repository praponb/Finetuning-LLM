from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city") == "Chiang Mai")
    .select(
        "id",
        "supplier_name",
        F.substring("city", 1, 4).alias("city_prefix"),
        F.length("supplier_name").alias("supplier_name_length"),
    )
)
