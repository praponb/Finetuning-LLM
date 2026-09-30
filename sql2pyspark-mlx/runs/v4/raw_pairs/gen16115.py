from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.year("opened_date").alias("opened_year"))
    .agg(
        F.min("sqft").alias("min_sqft"),
        F.min("opened_date").alias("min_opened_date"),
    )
)
