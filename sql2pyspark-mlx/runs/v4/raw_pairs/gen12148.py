from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "country",
        "supplier_name",
        F.round((F.col("rating") * 100), 2).alias("adjusted_rating"),
    )
)
