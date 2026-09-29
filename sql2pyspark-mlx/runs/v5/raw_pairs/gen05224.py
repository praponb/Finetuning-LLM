from pyspark.sql import functions as F

result = (
    df_shipments
    .cube(F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"))
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sort_array(F.collect_set("carrier")).alias("carrier_list"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.count(F.when(F.col("carrier") == "Flash", 1)).alias("flash_count"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("cost_per_ups"),
    )
)
