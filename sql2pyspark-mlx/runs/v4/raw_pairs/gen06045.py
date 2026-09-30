from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(
        F.year("opened_date").alias("opened_year"),
        F.date_trunc("MONTH", "opened_date").alias("opened_month"),
    )
    .agg(F.sum(F.when(F.col("region") == "South", 1).otherwise(0)).alias("south_count"))
    .orderBy(F.col("opened_year").desc(), "opened_month")
)
