from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy("order_id")
    .agg(
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.avg(F.when(F.col("carrier") == "Flash", F.col("cost"))).alias("flash_avg_cost"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
    )
    .orderBy(F.col("flash_avg_cost").desc())
)
