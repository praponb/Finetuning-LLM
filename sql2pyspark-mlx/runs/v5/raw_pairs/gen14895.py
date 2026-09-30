from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.coalesce("supplier_name", F.lit("N/A")).alias("supplier_name_filled"),
        F.ceil("rating").alias("rating_whole"),
    )
    .orderBy(F.col("id").desc())
)
