from pyspark.sql import functions as F

src = df_suppliers.filter(F.col("country") != "JP")

suppliers_stats = (
    src
    .groupBy("city")
    .agg(
        F.round(((100 * F.sum(F.when(F.col("city") == "London", 1).otherwise(0))) / F.count("*")), 1).alias("pct_london"),
    )
)

result = suppliers_stats.orderBy(F.col("pct_london").desc())
