from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("city") == "Chiang Mai") | F.col("supplier_name").like("%Tok%"))
    .agg(
        F.sum("rating").alias("total_rating"),
        F.count("*").alias("supplier_count"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .orderBy(F.col("total_rating").desc(), "stddev_rating")
)
