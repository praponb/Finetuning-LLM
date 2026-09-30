from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.count("sqft").alias("sqft_count"),
        (F.max("sqft") - F.min("sqft")).alias("sqft_range"),
    )
    .filter(F.col("sqft_count") < 3)
)
