from pyspark.sql import functions as F

result = (
    df_inventory
    .filter(F.col("last_restock") >= "2023-10-02")
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.stddev("quantity"), 2).alias("stddev_quantity"),
    )
)
