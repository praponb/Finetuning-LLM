from pyspark.sql import functions as F

result = (
    df_inventory
    .groupBy(F.date_trunc("year", "last_restock").alias("year_start"), "product_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", 0.9).alias("quantity_p90"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .orderBy(F.col("year_start").desc_nulls_last())
)
