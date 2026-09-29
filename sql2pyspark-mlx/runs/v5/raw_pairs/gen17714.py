from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country") != "DE")
    .groupBy(
        F.when(F.col("country").isin(["UK", "SG"]), "tier_1").otherwise("other").alias("country_group"),
    )
    .agg(
        F.round(((100 * F.sum(F.when(F.col("country") == "US", 1).otherwise(0))) / F.count("*")), 1).alias("pct_us"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("pct_us") > 60)
    .orderBy(F.col("stddev_rating").desc())
)
