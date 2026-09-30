from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("sqft").desc_nulls_last(), "id")
w2 = Window.partitionBy("city", "region").orderBy("region_length", "id")
w3 = Window.orderBy(F.col("seq_no").desc_nulls_last())
w4 = Window.orderBy("next_region_length", "id").rowsBetween(-5, Window.currentRow)
w5 = Window.orderBy("prev_region_length", "id").rowsBetween(-2, Window.currentRow)
w6 = Window.orderBy("next_region_length", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_stores
    .filter(F.col("opened_date") > F.lit("2023-04-21").cast("date"))
    .withColumn("region_length", F.length("region"))
)

deduped = (
    base
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

scored = (
    deduped
    .filter(F.length("store_name") > 6)
    .select(
        "id",
        "opened_date",
        F.lag("sqft").over(w2).alias("prev_sqft"),
        F.lead("sqft").over(w2).alias("next_sqft"),
        F.lag("region_length").over(w2).alias("prev_region_length"),
        F.lead("region_length").over(w2).alias("next_region_length"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "prev_sqft",
        "next_sqft",
        "prev_region_length",
        F.dense_rank().over(w3).alias("seq_no_dense_rank"),
        F.avg("next_region_length").over(w4).alias("next_region_length_avg_5"),
        F.avg("next_region_length").over(w5).alias("next_region_length_avg_2"),
        F.last("prev_region_length", ignorenulls=True).over(w6).alias("last_known_prev_region_length"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("last_known_prev_region_length") >= 85, "HIGH").otherwise("LOW").alias("last_known_prev_region_length_tier"),
        F.when(F.col("prev_region_length") >= 69, "HIGH").otherwise("LOW").alias("prev_region_length_tier"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.max("prev_region_length").alias("max_prev_region_length"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("next_region_length_avg_2", 0.95).alias("next_region_length_avg_2_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
    .filter(F.col("max_prev_region_length") >= 0)
    .orderBy(F.col("distinct_ids_3").asc_nulls_last())
)
