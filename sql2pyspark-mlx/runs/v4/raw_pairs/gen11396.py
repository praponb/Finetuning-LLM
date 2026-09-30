from pyspark.sql import functions as F

cleaned = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.substring("supplier_name", 1, 4).alias("supplier_name_prefix"),
    )
)

result = (
    cleaned
    .groupBy("city")
    .pivot("country", ["SG", "UK", "US"])
    .agg(F.sum("rating"))
    .select(
        "city",
        F.col("SG").alias("sg"),
        F.col("UK").alias("uk"),
        F.col("US").alias("us"),
    )
)
