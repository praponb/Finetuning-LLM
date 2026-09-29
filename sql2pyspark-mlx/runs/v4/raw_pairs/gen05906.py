from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("city").isNull())
    .select(
        "id",
        "city",
        "rating",
        "country",
        F.when(F.col("rating") >= 3.7, "High").when(F.col("rating") >= 2.4, "Medium").otherwise("Low").alias("rating_tier"),
        F.initcap(F.trim("country")).alias("country_title"),
    )
    .orderBy("country_title", "id")
    .limit(50)
)
