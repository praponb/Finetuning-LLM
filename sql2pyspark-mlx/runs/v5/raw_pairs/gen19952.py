from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") == "UK")
    .select(
        "id",
        "country",
        "supplier_name",
        F.round("rating", 0).alias("rating_rounded"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
    .orderBy("country")
)
