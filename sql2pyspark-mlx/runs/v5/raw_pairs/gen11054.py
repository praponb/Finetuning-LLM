from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region")
w2 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-3, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("region").orderBy("opened_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("region").orderBy("city_sum_sqft", "id")
w6 = Window.partitionBy("region").orderBy("row_num", "id")
w7 = Window.orderBy("sqft_avg_6_prior_share")
w8 = Window.partitionBy("region").orderBy(F.col("next_sqft_avg_3").desc_nulls_last())
w9 = Window.partitionBy("region").orderBy("sqft_avg_6_prior_share", "id")

filtered = (
    df_stores
    .filter(F.col("opened_date") > F.to_date(F.lit("2023-09-03")))
    .select(
        "id",
        "store_name",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.concat("city", F.lit(" - "), "region").alias("city_region"),
        F.coalesce("region", F.lit("N/A")).alias("region_filled"),
    )
)

metrics = (
    filtered
    .select(
        "id",
        "region",
        "city_region",
        "region_filled",
        F.count("sqft").over(w).alias("region_count_sqft"),
        F.avg("sqft").over(w2).alias("sqft_avg_3"),
        F.sum("sqft").over(w3).alias("city_sum_sqft"),
        F.avg("sqft").over(w4).alias("sqft_avg_6_prior"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

metrics_stores = (
    deduped
    .select(
        "id",
        "region",
        "region_filled",
        "sqft_avg_3",
        "row_num",
        F.lead("city_sum_sqft").over(w6).alias("next_city_sum_sqft"),
        F.lag("city_sum_sqft").over(w6).alias("prev_city_sum_sqft"),
        F.lag("sqft_avg_3").over(w6).alias("prev_sqft_avg_3"),
        F.lead("sqft_avg_3").over(w6).alias("next_sqft_avg_3"),
        F.row_number().over(w6).alias("row_seq"),
        F.round((F.col("sqft_avg_6_prior") / F.sum("sqft_avg_6_prior").over(w)), 4).alias("sqft_avg_6_prior_share"),
    )
)

enriched = (
    metrics_stores
    .select(
        "*",
        F.dense_rank().over(w7).alias("sqft_avg_6_prior_share_dense_rank"),
        F.round((F.col("sqft_avg_3") / F.sum("sqft_avg_3").over(w)), 4).alias("sqft_avg_3_share"),
        F.percent_rank().over(w8).alias("next_sqft_avg_3_percent_rank"),
        F.row_number().over(w9).alias("seq_no"),
    )
)

daily = (
    enriched
    .groupBy(
        F.when(F.col("sqft_avg_3") >= 13637, "HIGH").otherwise("LOW").alias("sqft_avg_3_tier"),
        F.when(F.col("prev_sqft_avg_3") >= 17766, "LARGE").otherwise("SMALL").alias("prev_sqft_avg_3_tier"),
        "region",
    )
    .agg(
        F.size(F.collect_set("region")).alias("num_regions"),
        F.percentile_approx("next_city_sum_sqft", [0.25, 0.5, 0.75]).alias("next_city_sum_sqft_quartiles"),
        F.count_if(F.col("region") == "Central").alias("central_events"),
    )
)

result = daily.select("sqft_avg_3_tier", "prev_sqft_avg_3_tier", "region", "central_events")
