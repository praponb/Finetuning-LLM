from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("city").isin(["Chiang Mai", "London"]) & (F.length("store_name") < 10))
    .select(
        "sqft",
        "store_name",
        F.round("sqft", 2).alias("sqft_rounded"),
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
    .orderBy("sqft_rounded")
)
