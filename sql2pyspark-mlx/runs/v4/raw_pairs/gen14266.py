from pyspark.sql import functions as F

result = (
    df_suppliers
    .filter(F.col("country").isin(["US", "SG", "DE"]) & ((F.col("rating") <= 2.6) | (F.col("rating") <= 2.3)))
    .groupBy(
        F.when(F.col("city").isin(["New York", "Singapore"]), "group_a").otherwise("other").alias("city_group"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("rating"), 2).alias("stddev_rating"),
    )
    .filter(F.col("stddev_rating") < 34)
    .orderBy("city_group")
)
