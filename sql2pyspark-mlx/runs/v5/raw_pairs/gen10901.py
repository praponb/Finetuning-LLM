from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "city",
        "country",
        "rating",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
    .orderBy(F.col("id").desc())
)
