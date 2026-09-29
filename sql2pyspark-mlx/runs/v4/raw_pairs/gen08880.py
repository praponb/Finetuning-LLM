from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 212, "LARGE").otherwise("SMALL").alias("quantity_tier"),
    )
    .agg(
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.avg("quantity").alias("avg_quantity"),
        F.bit_and("quantity").alias("quantity_bit_and_2"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
    )
    .orderBy(F.col("quantity_bit_and_2").desc_nulls_last())
)
