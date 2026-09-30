from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy("last3_sum_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("last3_max_rating", "id")
w4 = Window.partitionBy("country", "city").orderBy(F.col("last3_avg_rating").desc_nulls_last())
w_recent = Window.partitionBy("country").orderBy("last3_max_rating", "id")
w6 = Window.partitionBy("city").orderBy("last3_max_rating", "id")

cleaned = (
    df_suppliers
    .filter(F.col("supplier_name").like("%Globa%"))
    .select(
        "*",
        F.trim("country").alias("clean_country"),
        F.concat_ws("-", "supplier_name", "country").alias("supplier_name_country_key"),
        F.coalesce("city", F.lit("UNKNOWN")).alias("city_clean"),
    )
)

scored = (
    cleaned
    .filter(F.col("supplier_name").like("%Gmb%"))
    .withColumn("last3_avg_rating", F.avg("rating").over(w))
    .withColumn("last3_count_rating", F.count("rating").over(w))
    .withColumn("last3_sum_rating", F.sum("rating").over(w))
    .withColumn("last3_max_rating", F.max("rating").over(w))
)

cleaned_suppliers = (
    scored
    .filter(F.col("rating") < 2.0)
    .select("*", F.ceil("last3_sum_rating").alias("last3_sum_rating_whole"))
)

enriched = (
    cleaned_suppliers
    .select(
        "id",
        "country",
        "rating",
        "city_clean",
        "last3_max_rating",
        F.first("rating", ignorenulls=True).over(w2).alias("first_known_rating"),
        F.lead("last3_sum_rating_whole", 2).over(w3).alias("next_last3_sum_rating_whole"),
        F.cume_dist().over(w4).alias("last3_avg_rating_cume_dist"),
        F.row_number().over(w_recent).alias("seq_no"),
        F.ntile(4).over(w6).alias("last3_max_rating_bucket"),
    )
)

result = (
    enriched
    .groupBy(
        F.when(F.col("last3_max_rating") >= 4.2, "HIGH").otherwise("LOW").alias("last3_max_rating_tier"),
        F.when(F.col("rating") >= 4.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
    )
    .agg(
        F.count_if(F.col("country") == "SG").alias("sg_events"),
        F.count(F.when(F.col("country") == "DE", 1)).alias("de_count"),
        F.count(F.when(F.col("country") == "US", 1)).alias("us_count"),
        F.sum(F.when(F.col("country") == "TH", F.col("seq_no")).otherwise(0)).alias("th_seq_no"),
        F.percentile_approx("last3_avg_rating_cume_dist", [0.25, 0.5, 0.75]).alias("last3_avg_rating_cume_dist_quartiles"),
    )
    .orderBy("sg_events")
)
