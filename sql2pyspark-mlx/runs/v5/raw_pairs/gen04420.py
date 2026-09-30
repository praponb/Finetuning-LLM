from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_format("opened_date", "yyyy-MM").alias("opened_month"))
    .agg(
        F.count("*").alias("cnt"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
