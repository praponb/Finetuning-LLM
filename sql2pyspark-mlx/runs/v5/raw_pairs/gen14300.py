from pyspark.sql import functions as F

recent_suppliers = df_suppliers.filter(F.col("rating") > 3.1)

city_summary = (
    recent_suppliers
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "Chiang Mai", 1).otherwise(0))) / F.count("*")), 1).alias("pct_chiang_mai"),
    )
)

result = (
    city_summary
    .orderBy(F.col("pct_chiang_mai").desc(), "city")
    .limit(5)
)
