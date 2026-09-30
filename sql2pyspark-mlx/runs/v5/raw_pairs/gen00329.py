from pyspark.sql import functions as F

src = (
    df_stores
    .filter((F.col("opened_date") <= "2023-06-01") & F.upper("store_name").like("%IRP%") & (F.col("region") == "Central"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
)

stores_stats = (
    src
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.min("opened_date").alias("min_opened_date"),
        F.round(F.stddev("sqft"), 2).alias("stddev_sqft"),
    )
)

result = (
    stores_stats
    .filter(F.col("stddev_sqft") > 61)
    .select("opened_month", "min_opened_date", "stddev_sqft")
)
