from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["DE", "UK"]) & (F.col("rating") <= 3.5))
    .agg(
        F.count(F.when(F.col("country") == "SG", 1)).alias("sg_count"),
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
    )
    .orderBy(F.col("sg_count").desc())
)
