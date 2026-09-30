from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 242, "PREMIUM").otherwise("STANDARD").alias("quantity_tier"),
    )
    .agg(
        F.avg("quantity").alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .filter(F.col("quantity_bit_and") < 1)
    .orderBy("quantity_bit_or")
)
