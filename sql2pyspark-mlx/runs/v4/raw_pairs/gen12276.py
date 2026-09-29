from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("sqft_tier").orderBy(F.col("singapore_rows").desc())

deduped = (
    df_stores
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

result = (
    deduped
    .groupBy(
        F.when(F.col("sqft") >= 14207, "HIGH").otherwise("LOW").alias("sqft_tier"),
        F.date_trunc("YEAR", "opened_date").alias("year_start"),
    )
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.percentile_approx("sqft", 0.95).alias("sqft_p95"),
    )
    .withColumn("singapore_rows_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("singapore_rows").desc())
)
