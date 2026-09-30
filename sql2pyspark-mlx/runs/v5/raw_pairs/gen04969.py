from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("country").orderBy("rating_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("city").orderBy("rating_scaled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city", "country").orderBy("running_max_rating_scaled", "id")

filtered = (
    df_suppliers
    .filter(F.col("city") == "Tokyo")
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.round((F.col("rating") * 1.15), 2).alias("rating_scaled"),
        F.when(F.col("city").isin(["Berlin", "Bangkok"]), "Core").otherwise("Other").alias("city_group"),
    )
)

enriched = (
    filtered
    .select(
        "id",
        "country",
        "city",
        "rating",
        "rating_scaled",
        "city_group",
        F.first("city", ignorenulls=True).over(w_hist).alias("first_known_city"),
        F.first("city", ignorenulls=True).over(w_hist).alias("first_known_city_2"),
    )
)

with_windows = (
    enriched
    .withColumn("running_min_rating_scaled", F.min("rating_scaled").over(w_running))
    .withColumn("running_count_rating_scaled", F.count("rating_scaled").over(w_running))
    .withColumn("running_max_rating_scaled", F.max("rating_scaled").over(w_running))
)

enriched_suppliers = (
    with_windows
    .select(
        "id",
        "city",
        "rating_scaled",
        "city_group",
        "first_known_city",
        "running_min_rating_scaled",
        F.lag("rating_scaled").over(w3).alias("prev_rating_scaled"),
        F.lead("rating_scaled").over(w3).alias("next_rating_scaled"),
        F.lag("running_min_rating_scaled").over(w3).alias("prev_running_min_rating_scaled"),
        F.lead("running_min_rating_scaled").over(w3).alias("next_running_min_rating_scaled"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

grouped = (
    enriched_suppliers
    .withColumn("next_rating_scaled_tier", F.when(F.col("next_rating_scaled") >= 61, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["next_rating_scaled_tier", "seq_no"], ["next_rating_scaled_tier"]],
        "next_rating_scaled_tier",
        "seq_no",
    )
    .agg(
        F.percentile_approx("prev_running_min_rating_scaled", 0.9).alias("prev_running_min_rating_scaled_p90"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count_if(F.col("city") == "Bangkok").alias("bangkok_events"),
        F.count("*").alias("cnt"),
        F.sum(F.when(F.col("city") == "New York", F.col("prev_running_min_rating_scaled")).otherwise(0)).alias("new_york_prev_running_min_rating_scaled"),
    )
)

result = (
    grouped
    .select(
        "next_rating_scaled_tier",
        "seq_no",
        "prev_running_min_rating_scaled_p90",
        "bangkok_events",
        "new_york_prev_running_min_rating_scaled",
    )
    .orderBy(F.col("bangkok_events").desc())
)
