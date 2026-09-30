from pyspark.sql import functions as F

result = (
    df_stores
    .agg(
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
        F.round(((100 * F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0))) / F.count("*")), 1).alias("pct_bangkok"),
    )
    .orderBy("pct_bangkok")
)
