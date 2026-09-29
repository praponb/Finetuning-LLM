from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("delivered_date") <= "2024-02-22")
    .groupBy("status")
    .agg(F.round(F.stddev("cost"), 2).alias("stddev_cost"))
    .filter(F.col("stddev_cost") < 29)
    .orderBy(F.col("stddev_cost").desc())
)
