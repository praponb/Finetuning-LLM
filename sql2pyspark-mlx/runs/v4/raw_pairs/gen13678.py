from pyspark.sql import functions as F

src = (
    df_suppliers
    .filter((F.col("rating") < 3.3) | F.col("city").isin(["Bangkok", "Singapore"]))
)

agg = (
    src
    .groupBy("city")
    .agg(F.round(F.avg("rating"), 2).alias("avg_rating"))
)

result = agg.filter(F.col("avg_rating") < 29)
