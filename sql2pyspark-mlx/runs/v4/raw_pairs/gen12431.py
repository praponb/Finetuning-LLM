from pyspark.sql import functions as F

src = (
    df_stores
    .filter(F.col("opened_date").between("2023-12-15", "2024-05-01") & (F.col("region") != "West"))
)

agg = (
    src
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"), "region")
    .agg(
        F.max("opened_date").alias("last_opened_date"),
        F.max("sqft").alias("max_sqft"),
    )
)

result = agg
