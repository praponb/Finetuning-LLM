from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(F.date_trunc("YEAR", "shipped_date").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("carrier") == "Flash", F.col("cost")).otherwise(0)).alias("flash_cost"),
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
    )
    .orderBy(F.col("cost_p90").desc())
)
