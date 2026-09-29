from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("YEAR", "last_restock").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
    )
    .filter(F.col("quantity_p90") < 0)
)
