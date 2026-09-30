from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating") > 3.8)
    .select(
        "id",
        "supplier_name",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
    )
)
