from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("rating").between(1.9, 3.0) & (F.col("country") != "SG"))
    .groupBy("country")
    .agg(F.round(F.stddev("rating"), 2).alias("stddev_rating"))
    .filter(F.col("stddev_rating") < 20)
)
