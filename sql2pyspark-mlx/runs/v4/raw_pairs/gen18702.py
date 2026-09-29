from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country", "city").orderBy("rating", "id")
w3 = Window.partitionBy("country", "city").orderBy(F.col("rating").desc(), "id")
w_recent = Window.partitionBy("city").orderBy("rating", "id")
w_recent_2 = Window.partitionBy("country").orderBy("rating", "id")
w6 = Window.partitionBy("country", "city").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("country").orderBy(F.col("rating_bucket").asc_nulls_last())
w8 = Window.partitionBy("city").orderBy("rating_bucket_3", "id")
w9 = Window.partitionBy("country", "city").orderBy("cumulative_rating_bucket", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w10 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy("rating_bucket_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("country").orderBy("rating_bucket_3", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w13 = Window.orderBy(F.col("rating").desc())
w14 = Window.partitionBy("country").orderBy(F.col("rating_percent_rank").desc(), "id")
w15 = Window.partitionBy("city").orderBy(F.col("event_seq").desc(), "id")
w16 = Window.partitionBy("city").orderBy("rating_bucket_percent_rank", "id")

valid = (
    df_suppliers
    .filter(F.col("city") != "London")
    .select(
        "id",
        "country",
        "city",
        "rating",
        F.concat_ws("-", "country", "supplier_name").alias("country_supplier_name_key"),
        F.coalesce("city", F.lit("OTHER")).alias("city_filled"),
    )
)

not_flagged = (
    valid
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.category") != "Sports"), "left_semi")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.launch_date") <= "2024-04-01"), "left_anti")
)

enriched = (
    not_flagged_suppliers
    .withColumn("running_rating", F.sum("rating").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w2))
    .withColumn("rating_bucket", F.ntile(3).over(w3))
    .withColumn("rating_bucket_2", F.ntile(4).over(w_recent))
    .withColumn("rating_bucket_3", F.ntile(3).over(w_recent_2))
)

scored = (
    enriched
    .withColumn("cumulative_rating_bucket", F.sum("rating_bucket").over(w6))
    .withColumn("rating_bucket_percent_rank", F.percent_rank().over(w7))
    .withColumn("prev_running_rating", F.lag("running_rating", 2).over(w8))
)

scored_suppliers = (
    scored
    .withColumn("last_known_running_rating", F.last("running_rating", ignorenulls=True).over(w9))
    .withColumn("last_known_city", F.last("city", ignorenulls=True).over(w10))
    .withColumn("cumulative_running_rating", F.sum("running_rating").over(w11))
    .withColumn("last_known_rating_bucket_3", F.last("rating_bucket_3", ignorenulls=True).over(w_running))
    .withColumn("rating_percent_rank", F.percent_rank().over(w13))
)

scored_suppliers_2 = (
    scored_suppliers
    .withColumn("seq_no", F.row_number().over(w14))
    .withColumn("event_seq_2", F.row_number().over(w15))
    .withColumn("rating_bucket_percent_rank_bucket", F.ntile(3).over(w16))
)

result = (
    scored_suppliers_2
    .cube("country")
    .agg(
        F.count(F.when(F.col("city") == "Singapore", 1)).alias("singapore_rows"),
        F.count("*").alias("row_count"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.round(F.avg("event_seq"), 2).alias("avg_event_seq"),
    )
    .filter(F.col("row_count") >= 1)
    .orderBy("avg_event_seq")
)
