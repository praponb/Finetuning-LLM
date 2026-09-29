from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("region") == "North")
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        "city",
        F.date_format("opened_date", "yyyy-MM").alias("opened_month"),
    )
    .agg(F.avg("sqft").alias("avg_sqft"))
    .orderBy("opened_month")
)
