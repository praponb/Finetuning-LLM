from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.when(F.col("quantity") >= 188, "HIGH").otherwise("LOW").alias("quantity_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("event_count"),
    )
    .orderBy(F.col("event_count").desc_nulls_first())
)
