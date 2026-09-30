from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "US")
    .groupBy("country")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
        F.round(F.avg("rating"), 2).alias("avg_rating"),
    )
    .orderBy(F.col("country").desc())
)
