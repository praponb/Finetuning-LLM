from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("store_name").like("Ma%"))
    .groupBy(F.month("opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.sum("sqft").alias("total_sqft"),
    )
)
