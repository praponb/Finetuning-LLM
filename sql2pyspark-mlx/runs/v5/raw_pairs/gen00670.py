from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Who%") & (F.col("rating") > 4.0))
    .select(
        "rating",
        F.ceil("rating").alias("rating_whole"),
        F.when(F.col("rating") >= 3.8, "High").when(F.col("rating") >= 2.1, "Medium").otherwise("Low").alias("rating_level"),
    )
)
