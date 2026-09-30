from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "JP"]) & F.col("supplier_name").like("Lio%"))
    .select(
        "country",
        "rating",
        "city",
        F.round("rating", 0).alias("rating_rounded"),
        F.substring("supplier_name", 1, 3).alias("supplier_name_prefix"),
    )
    .orderBy(F.col("rating_rounded").desc())
)
