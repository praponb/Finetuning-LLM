from pyspark.sql import functions as F

result = (
    df_suppliers
    .agg(
        F.count("*").alias("num_suppliers"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
)
