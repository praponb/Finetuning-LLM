from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.lower(F.trim("email")) == "jack@startup.io")
    .groupBy(
        F.when(F.col("country").isin(["SG", "UK"]), "core").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("segment") == "SMB", 1).otherwise(0))) / F.count("*")), 1).alias("pct_smb"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Berlin", 1).otherwise(0))) / F.count("*")), 1).alias("pct_berlin"),
        F.countDistinct("age").alias("unique_segments"),
    )
)
