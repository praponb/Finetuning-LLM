from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("region").isin(["West", "East", "South"]) & F.col("opened_date").between("2024-08-01", "2025-04-01") & F.col("region").isin(["North", "West", "East"]))
    .withColumn("city_length", F.length("city"))
)

opened_year_summary = (
    src
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(F.countDistinct("city_length").alias("unique_regions"))
)

result = opened_year_summary.select("opened_year", "unique_regions")
