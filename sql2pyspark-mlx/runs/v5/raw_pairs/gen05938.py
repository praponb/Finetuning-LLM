from pyspark.sql import functions as F

result = (
    df_suppliers
    .select(
        "id",
        "rating",
        "country",
        "city",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
    .orderBy(F.col("rating").desc(), F.col("id").desc())
)
