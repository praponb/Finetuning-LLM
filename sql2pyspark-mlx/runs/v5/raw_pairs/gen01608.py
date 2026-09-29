from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("supplier_name").isNotNull())
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Tokyo", F.col("rating")).otherwise(0)).alias("tokyo_rating"),
        F.round(((100 * F.sum(F.when(F.col("country") == "DE", 1).otherwise(0))) / F.count("*")), 1).alias("pct_de"),
    )
    .orderBy("pct_de")
)
