from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "TH"]))
    .groupBy("country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
    .filter(F.col("stddev_rating") >= 61)
    .select("country", "stddev_rating", "pct_chiang_mai")
    .orderBy("pct_chiang_mai", "country")
    .limit(5)
)
