from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("rating").between(1.8, 2.7))
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

result = (
    base
    .select("country", "id")
    .exceptAll(
        df_suppliers
        .filter(~F.col("supplier_name").like("Eur%"))
        .select("country", "id")
    )
)
