from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter((F.col("rating") > 2.0) & (F.col("country").isin(["SG", "TH", "DE"]) | (F.col("country") == "TH")))
    .agg(
        F.countDistinct("rating").alias("unique_countrys"),
        F.sum("rating").alias("total_rating"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
    )
)
