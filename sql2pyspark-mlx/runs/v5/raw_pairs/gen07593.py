from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy("store_id")
    .agg(
        F.max("quantity").alias("max_quantity"),
        F.avg("quantity").alias("avg_quantity"),
        F.count("*").alias("cnt"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity_2"),
        F.bit_or("quantity").alias("quantity_bit_or"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
)
