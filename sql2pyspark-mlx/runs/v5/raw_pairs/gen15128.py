from pyspark.sql import functions as F

valid_suppliers = df_suppliers.filter(F.col("rating") < 3.1)

agg = (
    valid_suppliers
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "London", 1).otherwise(0)).alias("london_count"),
        F.countDistinct("rating").alias("unique_countrys"),
    )
)

result = (
    agg
    .select("city", "london_count", "unique_countrys")
    .orderBy(F.col("unique_countrys").desc(), "city")
    .limit(20)
)
