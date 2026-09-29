from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.round(F.avg("quantity"), 2).alias("avg_quantity"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles_2"),
        F.bit_and("quantity").alias("quantity_bit_and"),
    )
    .orderBy(F.col("num_rows").desc())
)
