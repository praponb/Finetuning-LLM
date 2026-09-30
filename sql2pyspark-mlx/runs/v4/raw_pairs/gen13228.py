from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(
        F.date_trunc("YEAR", "last_restock").alias("year_start"),
        "product_id",
        "store_id",
    )
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.stddev_samp("quantity").alias("quantity_stddev_2"),
    )
)
