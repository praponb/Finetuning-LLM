from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("sale_date") <= "2024-05-27")
    .groupBy("product_id", "store_id")
    .agg(
        F.round(F.stddev("units"), 2).alias("stddev_units"),
        F.round(F.avg("units"), 2).alias("avg_units"),
    )
    .filter(F.col("avg_units") < 41)
)
