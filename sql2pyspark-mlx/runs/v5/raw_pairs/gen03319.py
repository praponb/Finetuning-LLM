from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(-3, Window.currentRow)
w_grp_2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("country").orderBy("last4_min_rating", "id")
w4 = Window.partitionBy("city").orderBy(F.col("last4_min_rating").desc())
w5 = Window.partitionBy("country")
w_hist = Window.partitionBy("city", "country").orderBy("next_last4_min_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("city").orderBy("last4_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("city", "country").orderBy("first_known_last4_max_rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("country").orderBy("next_last4_min_rating", "id")
w10 = Window.partitionBy("city")

enriched = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "country",
        "city",
        F.max("rating").over(w_grp).alias("last4_max_rating"),
        F.count("rating").over(w_grp).alias("last4_count_rating"),
        F.min("rating").over(w_grp).alias("last4_min_rating"),
        F.avg("rating").over(w_grp_2).alias("rating_avg_6_prior"),
    )
)

with_windows = (
    enriched
    .select(
        "*",
        F.lead("last4_min_rating", 2, 0).over(w3).alias("next_last4_min_rating"),
        F.cume_dist().over(w4).alias("last4_min_rating_cume_dist"),
        F.round((F.col("last4_min_rating") / F.sum("last4_min_rating").over(w5)), 4).alias("last4_min_rating_share"),
    )
)

with_windows_suppliers = (
    with_windows
    .withColumn("running_avg_last4_count_rating", F.avg("last4_count_rating").over(w_hist))
    .withColumn("running_min_last4_count_rating", F.min("last4_count_rating").over(w_hist))
    .withColumn("running_count_last4_count_rating", F.count("last4_count_rating").over(w_hist))
    .withColumn("running_sum_last4_count_rating", F.sum("last4_count_rating").over(w_hist))
    .withColumn("first_known_last4_max_rating", F.first("last4_max_rating", ignorenulls=True).over(w7))
)

enriched_suppliers = (
    with_windows_suppliers
    .select(
        "id",
        "last4_max_rating",
        "rating_avg_6_prior",
        "running_min_last4_count_rating",
        "first_known_last4_max_rating",
        F.sum("last4_count_rating").over(w8).alias("running_last4_count_rating"),
        F.ntile(3).over(w9).alias("next_last4_min_rating_bucket"),
        F.round((F.col("last4_max_rating") / F.sum("last4_max_rating").over(w10)), 4).alias("last4_max_rating_share"),
    )
)

result = (
    enriched_suppliers
    .groupBy(
        F.when(F.col("first_known_last4_max_rating") >= 4.1, "HIGH").otherwise("LOW").alias("first_known_last4_max_rating_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("running_min_last4_count_rating", [0.25, 0.5, 0.75]).alias("running_min_last4_count_rating_quartiles"),
        F.avg("running_last4_count_rating").alias("avg_running_last4_count_rating"),
        F.round(F.avg("first_known_last4_max_rating"), 2).alias("avg_first_known_last4_max_rating"),
    )
    .filter(F.col("distinct_ids") > 1)
    .orderBy(F.col("distinct_ids").desc())
)
