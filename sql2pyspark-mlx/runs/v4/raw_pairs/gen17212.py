from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last())
w4 = Window.partitionBy("city").orderBy("sqft", "id")

metrics = (
    df_stores
    .withColumn("first_known_region", F.first("region", ignorenulls=True).over(w_running))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w2))
    .withColumn("sqft_dense_rank", F.dense_rank().over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
)

base = (
    metrics
    .filter(F.col("opened_date") >= F.lit("2023-09-15").cast("date"))
    .withColumn("days_since_opened_date", F.datediff(F.current_date(), "opened_date"))
)

kept = (
    base
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.store_id") == F.col("s.id")) & (F.col("c.store_id").isin([1, 2, 4, 6])), "left_anti")
)

daily = (
    kept
    .groupBy(
        F.when(F.col("days_since_opened_date") >= 77, "PREMIUM").otherwise("STANDARD").alias("days_since_opened_date_tier"),
    )
    .agg(
        F.percentile_approx("sqft", [0.25, 0.5, 0.75]).alias("sqft_quartiles"),
        F.stddev_samp("sqft").alias("sqft_stddev"),
        F.avg("days_since_opened_date").alias("avg_days_since_opened_date"),
    )
    .filter(F.col("avg_days_since_opened_date") < 0)
)

result = (
    daily
    .select(
        "days_since_opened_date_tier",
        "sqft_quartiles",
        "sqft_stddev",
        "avg_days_since_opened_date",
    )
    .orderBy(F.col("avg_days_since_opened_date").desc_nulls_last())
)
