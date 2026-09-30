from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(3.1, 4.0))
    .select(
        "id",
        "city",
        "country",
        "supplier_name",
        F.when(F.col("city") == "Singapore", 1).otherwise(0).alias("is_singapore"),
        F.round((F.col("rating") * 1.15), 2).alias("adjusted_rating"),
    )
)
