from pyspark.sql import functions as F

base = (
    df_stores
    .filter((F.col("opened_date") >= F.to_date(F.lit("2024-02-23"))) & F.col("region").isNotNull() & F.col("sqft").isNotNull())
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "sqft",
        F.last_day("opened_date").alias("opened_date_month_end"),
    )
)

result = (
    base
    .groupBy("region")
    .pivot("city", ["Chiang Mai", "New York", "Singapore"])
    .agg(F.max("sqft"))
    .select(
        "region",
        F.col("Chiang Mai").alias("chiang_mai"),
        F.col("New York").alias("new_york"),
        F.col("Singapore").alias("singapore"),
    )
)
