from pyspark.sql import functions as F

filtered = (
    df_inventory
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-02-25")))
    .withColumn("last_restock_period", F.date_trunc("month", "last_restock"))
)

result = (
    filtered
    .groupBy("store_id")
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
    )
    .orderBy(F.col("avg_quantity").asc_nulls_first())
)
