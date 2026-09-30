from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") >= 4.2)
    .select(
        "id",
        "country",
        F.coalesce("city", F.lit("unassigned")).alias("city_filled"),
    )
)
