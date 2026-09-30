from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("rating").between(3.6, 4.2) | (F.col("country") == "DE"))

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.sum("rating").alias("total_rating"),
        F.min("rating").alias("min_rating"),
    )
)

result = suppliers_stats.select("city", "total_rating", "min_rating")
