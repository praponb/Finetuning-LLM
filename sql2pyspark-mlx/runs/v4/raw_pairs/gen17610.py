from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(-2, -1)
w2 = Window.partitionBy("region").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("sqft", "id")

metrics = (
    df_stores
    .withColumn("sqft_avg_2_prior", F.avg("sqft").over(w))
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w2))
    .withColumn("sqft_bucket", F.ntile(5).over(w3))
)

cleaned = (
    metrics
    .filter((F.col("opened_date") > F.lit("2024-01-15").cast("date")) & F.col("region").isNotNull())
    .select("*", F.ceil("first_known_sqft").alias("first_known_sqft_whole"))
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.store_id") == F.col("s.id")) & (F.col("x.sale_date") >= "2025-08-25"), "left_semi")
)

result = (
    not_flagged
    .cube(
        F.when(F.col("sqft") >= 14748, "LARGE").otherwise("SMALL").alias("sqft_tier"),
        F.date_trunc("QUARTER", "opened_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.sum("first_known_sqft_whole") / F.nullif(F.count(F.when(F.col("region") == "West", 1)), F.lit(0)), 2).alias("first_known_sqft_whole_per_west"),
        F.count("*").alias("row_count"),
        F.count_if(F.col("city") == "New York").alias("new_york_events"),
    )
    .orderBy(F.col("new_york_events").desc_nulls_last())
)
