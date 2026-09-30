from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.col("cost") >= 82, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("cost_per_ups"),
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.round(F.avg("cost"), 2).alias("avg_cost_2"),
    )
    .orderBy(F.col("avg_cost").desc_nulls_first())
)
