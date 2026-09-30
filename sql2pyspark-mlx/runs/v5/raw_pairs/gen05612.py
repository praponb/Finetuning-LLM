from pyspark.sql import functions as F

src = (
    df_shipments
    .filter(F.year("shipped_date") >= 2024)
    .withColumn("delivered_date_month_end", F.last_day("delivered_date"))
)

summary = (
    src
    .groupBy("status")
    .agg(
        F.round(F.stddev("cost"), 2).alias("stddev_cost"),
        F.max("cost").alias("max_cost"),
    )
)

result = (
    summary
    .filter(F.col("max_cost") > 77)
    .select("status", "stddev_cost", "max_cost")
    .orderBy("max_cost")
)
