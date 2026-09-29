from pyspark.sql import functions as F

eligible = (
    df_stores
    .alias("s")
    .join(df_orders.alias("ch"), (F.col("ch.store_id") == F.col("s.id")) & (F.col("ch.order_date") <= F.col("s.opened_date")), "left_anti")
)

result = (
    eligible
    .groupBy(
        F.when(F.col("sqft") >= 15918, "HIGH").otherwise("LOW").alias("sqft_tier"),
        "city",
    )
    .agg(
        F.round(F.avg("sqft"), 2).alias("avg_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("city") == "London", 1)), F.lit(0)), 2).alias("sqft_per_london"),
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
    )
    .filter(F.col("avg_sqft") < 1000000)
    .orderBy(F.col("city").desc())
)
