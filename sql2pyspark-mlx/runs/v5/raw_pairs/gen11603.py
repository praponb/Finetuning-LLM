from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") <= 3.7) & (F.col("country") != "DE"))
    .select(
        "id",
        "supplier_name",
        F.when(F.col("city") == "London", 1).otherwise(0).alias("is_london"),
    )
)
