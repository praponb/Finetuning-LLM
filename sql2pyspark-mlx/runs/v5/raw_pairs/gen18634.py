from pyspark.sql import functions as F

base = (
    df_suppliers
    .filter(F.col("city").eqNullSafe(F.lit("Singapore")))
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.when(F.col("country").isin(["US", "SG"]), "Core").otherwise("Other").alias("country_group"),
    )
)

result = (
    base
    .select("id")
    .subtract(
        df_suppliers
        .filter(F.col("city") == "Bangkok")
        .select("id")
    )
)
