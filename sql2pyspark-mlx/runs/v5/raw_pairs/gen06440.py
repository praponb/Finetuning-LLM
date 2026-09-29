from pyspark.sql import functions as F

src = df_suppliers.filter(F.length("supplier_name") >= 10)

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "Bangkok", 1).otherwise(0)).alias("bangkok_count"),
        F.count("rating").alias("rating_count"),
    )
)

result = suppliers_stats.select("city", "bangkok_count", "rating_count")
