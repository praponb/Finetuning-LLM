from pyspark.sql import functions as F

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-05-25").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.last_day("last_restock").alias("last_restock_month_end"),
    )
)

result = (
    base
    .rollup("product_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.count("*").alias("event_count"),
    )
)
