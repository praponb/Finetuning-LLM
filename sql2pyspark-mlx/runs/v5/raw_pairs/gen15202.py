from pyspark.sql import functions as F

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & (F.col("country") == "US"))
    .select(
        "*",
        F.when(F.col("rating") >= 4.3, "High").when(F.col("rating") >= 1.5, "Medium").otherwise("Low").alias("rating_level"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

result = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost") < 450), "left_semi")
)
