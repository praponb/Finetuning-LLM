from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "city")
    .agg(
        F.count("sqft").alias("sqft_count"),
        F.countDistinct("sqft").alias("unique_regions"),
    )
)
