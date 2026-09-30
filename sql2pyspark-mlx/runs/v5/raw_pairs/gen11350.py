from pyspark.sql import functions as F

joined = (
    df_inventory
    .alias("e")
    .join(df_stores.alias("s"), F.col("e.store_id") == F.col("s.id"), "left")
    .select("e.*", F.col("s.store_name"), F.col("s.opened_date"))
)

result = (
    joined
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.percentile_approx("quantity", 0.5).alias("quantity_p50"),
    )
    .orderBy(F.col("quantity_p95").desc_nulls_last())
)
