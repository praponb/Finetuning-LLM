from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(F.date_trunc("WEEK", "sale_date").alias("week_start"), "store_id")
    .agg(
        F.count("*").alias("cnt"),
        F.stddev_samp("units").alias("units_stddev"),
        F.stddev_samp("revenue").alias("revenue_stddev"),
    )
)
