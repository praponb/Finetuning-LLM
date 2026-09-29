from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(~(F.col("supplier_name").like("%Wholes%") & (F.col("rating") <= 3.3)))
    .select(
        "id",
        "country",
        "rating",
        "supplier_name",
        F.substring("country", 1, 3).alias("country_prefix"),
    )
    .orderBy("country", "id")
    .limit(5)
)
