from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        F.round((F.col("rating") * 0.85), 2).alias("adjusted_rating"),
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)
