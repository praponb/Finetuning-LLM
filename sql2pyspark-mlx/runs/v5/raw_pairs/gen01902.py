from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id")
w2 = Window.partitionBy("country").orderBy("rating_whole", "id")
w3 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, Window.currentRow)
w4 = Window.partitionBy("country").orderBy("rating_whole_avg_6", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.orderBy("running_min_next_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("running_avg_next_rating", "id")
w_ordered_2 = Window.partitionBy("country")
w_part = Window.partitionBy("country").orderBy(F.col("running_count_next_rating").desc(), "id")

filtered = (
    df_suppliers
    .filter(F.col("supplier_name").like("Glo%"))
    .select(
        "id",
        "country",
        "rating",
        F.ceil("rating").alias("rating_whole"),
        F.coalesce("city", F.lit("OTHER")).alias("city_clean"),
    )
)

scored = (
    filtered
    .filter(F.col("country") == "JP")
    .select(
        "id",
        "country",
        "city_clean",
        F.row_number().over(w).alias("event_seq"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
        F.avg("rating_whole").over(w3).alias("rating_whole_avg_6"),
    )
)

enriched = (
    scored
    .withColumn("running_avg_next_rating", F.avg("next_rating").over(w4))
    .withColumn("running_count_next_rating", F.count("next_rating").over(w4))
    .withColumn("running_min_next_rating", F.min("next_rating").over(w4))
)

with_windows = (
    enriched
    .select(
        "*",
        F.first("rating_whole_avg_6", ignorenulls=True).over(w_ordered).alias("first_known_rating_whole_avg_6"),
        F.ntile(3).over(w6).alias("running_avg_next_rating_bucket"),
        F.min("next_rating").over(w_ordered_2).alias("country_min_next_rating"),
        F.ntile(10).over(w_part).alias("running_count_next_rating_bucket"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("event_seq") >= 8, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        "country",
        F.when(F.col("running_avg_next_rating") >= 4.4, "HIGH").otherwise("LOW").alias("running_avg_next_rating_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.size(F.collect_set("country")).alias("num_countries"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_count"),
        F.count("*").alias("event_count"),
    )
)
