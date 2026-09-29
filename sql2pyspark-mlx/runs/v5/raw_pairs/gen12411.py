from pyspark.sql import functions as F

result = (
    df_stores
    .filter(((F.col("sqft") <= 10309) & (F.col("city") == "Chiang Mai")) | (F.col("opened_date") < "2023-04-01"))
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.max("opened_date").alias("last_opened_date"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy(
        F.col("total_sqft").desc(),
        F.col("last_opened_date").desc(),
        "opened_year",
    )
    .limit(10)
)
