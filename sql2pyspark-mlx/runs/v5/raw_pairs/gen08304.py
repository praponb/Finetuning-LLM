from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy("store_id", F.year("sale_date").alias("sale_year"))
    .agg(
        F.count("*").alias("total_count"),
        F.round(F.stddev("units"), 2).alias("stddev_units"),
    )
)
