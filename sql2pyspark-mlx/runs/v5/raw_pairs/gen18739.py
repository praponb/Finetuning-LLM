from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy("rating", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("country").orderBy("rating", "id")
w_hist = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy(F.col("running_max_running_rating_2").desc(), "id")

valid = (
    df_suppliers
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        "rating",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
    )
)

enriched = (
    valid
    .withColumn("running_rating", F.sum("rating").over(w_recent))
    .withColumn("running_rating_2", F.sum("rating").over(w_recent))
    .withColumn("rating_avg_2", F.avg("rating").over(w2))
    .withColumn("next_rating", F.lead("rating", 1).over(w3))
)

enriched_suppliers = (
    enriched
    .filter(F.col("rating") <= 4.0)
    .select(
        "*",
        F.max("running_rating_2").over(w_hist).alias("running_max_running_rating_2"),
        F.count("running_rating_2").over(w_hist).alias("running_count_running_rating_2"),
        F.avg("running_rating_2").over(w_hist).alias("running_avg_running_rating_2"),
        F.sum("running_rating_2").over(w_hist).alias("running_sum_running_rating_2"),
    )
)

deduped = (
    enriched_suppliers
    .withColumn("dedup_rank", F.row_number().over(w5))
    .filter(F.col("dedup_rank") == 1)
)

daily = (
    deduped
    .groupBy(
        F.when(F.col("rating") >= 3.6, "PREMIUM").otherwise("STANDARD").alias("rating_tier"),
        "city",
    )
    .agg(
        F.count("*").alias("row_count"),
        F.count("*").alias("row_count_2"),
        F.percentile_approx("running_avg_running_rating_2", [0.25, 0.5, 0.75]).alias("running_avg_running_rating_2_quartiles"),
    )
)

result = (
    daily
    .select("rating_tier", "city", "running_avg_running_rating_2_quartiles")
    .orderBy(F.col("rating_tier").desc_nulls_first())
)
