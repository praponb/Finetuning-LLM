from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").asc_nulls_last(), "id")
w2 = Window.orderBy("sqft", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("city", "region").orderBy("sqft_avg_5_prior", "id").rowsBetween(-3, -1)
w5 = Window.partitionBy("region").orderBy("rank_in_group", "id")
w6 = Window.partitionBy("city").orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("sqft_avg_5_prior", "id").rowsBetween(-6, Window.currentRow)
w8 = Window.orderBy("city_min_sqft", "id")
w9 = Window.orderBy("sqft_avg_5_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.partitionBy("city", "region")

ranked = (
    df_stores
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

metrics = (
    ranked
    .withColumn("sqft_avg_5_prior", F.avg("sqft").over(w2))
    .withColumn("city_min_sqft", F.min("sqft").over(w3))
)

enriched = (
    metrics
    .withColumn("sqft_avg_3_prior", F.avg("sqft").over(w4))
    .withColumn("seq_no", F.row_number().over(w5))
)

metrics_stores = (
    enriched
    .withColumn("first_known_sqft", F.first("sqft", ignorenulls=True).over(w6))
    .withColumn("sqft_avg_6", F.avg("sqft").over(w7))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("running_sqft_avg_5_prior", F.sum("sqft_avg_5_prior").over(w9))
    .withColumn("sqft_avg_3_prior_share", F.round((F.col("sqft_avg_3_prior") / F.sum("sqft_avg_3_prior").over(w10)), 4))
)

grouped = (
    metrics_stores
    .groupBy("city", F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
        F.sum(F.when(F.col("region") == "North", F.col("sqft_avg_5_prior")).otherwise(0)).alias("north_sqft_avg_5_prior"),
        F.avg("sqft_avg_5_prior").alias("avg_sqft_avg_5_prior"),
    )
)

result = (
    grouped
    .select("city", "quarter_start", "avg_seq_no")
    .orderBy(F.col("city").desc())
)
