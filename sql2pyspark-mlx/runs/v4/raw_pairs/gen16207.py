from pyspark.sql import functions as F

src = (
    df_stores
    .filter(~(F.col("store_name").like("Mal%") & (F.col("sqft").isNotNull() | (F.col("region") == "East"))))
)

summary = (
    src
    .groupBy("region")
    .agg(
        F.avg("sqft").alias("avg_sqft"),
        F.min("opened_date").alias("first_opened_date"),
    )
)

result = (
    summary
    .filter(F.col("avg_sqft") < 15800)
    .orderBy("first_opened_date")
)
