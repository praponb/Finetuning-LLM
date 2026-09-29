from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(((F.col("country") == "SG") & (F.col("city") != "Bangkok")) | (F.col("rating") <= 2.5))
    .groupBy("country")
    .agg(
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
        (F.max("rating") - F.min("rating")).alias("rating_range"),
    )
)
