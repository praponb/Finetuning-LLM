from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w2 = Window.partitionBy("country").orderBy("rating", "id")
w3 = Window.partitionBy("city")
w4 = Window.orderBy("rating_share", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("next_rating", "id")

kept = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.cost") > 570), "left_semi")
)

with_windows = (
    kept
    .select(
        "id",
        "rating",
        F.avg("rating").over(w).alias("rating_avg_5_prior"),
        F.lead("rating", 1).over(w2).alias("next_rating"),
        F.round((F.col("rating") / F.sum("rating").over(w3)), 4).alias("rating_share"),
    )
)

eligible = (
    with_windows
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("x.tags"), "bestseller")), "left_semi")
)

scored = (
    eligible
    .select(
        "id",
        "rating_avg_5_prior",
        "rating_share",
        F.max("rating_share").over(w4).alias("running_max_rating_share"),
        F.sum("rating_share").over(w4).alias("running_sum_rating_share"),
        F.count("rating_share").over(w4).alias("running_count_rating_share"),
        F.lag("rating_share", 1).over(w5).alias("prev_rating_share"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("rating_avg_5_prior") >= 4, "HIGH").otherwise("LOW").alias("rating_avg_5_prior_tier"),
        F.when(F.col("prev_rating_share") >= 0.86, "HIGH").otherwise("LOW").alias("prev_rating_share_tier"),
    )
    .agg(
        F.percentile_approx("running_max_rating_share", [0.25, 0.5, 0.75]).alias("running_max_rating_share_quartiles"),
        F.round(F.avg("running_count_rating_share"), 2).alias("avg_running_count_rating_share"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("rating_share", [0.25, 0.5, 0.75]).alias("rating_share_quartiles"),
        F.round(F.avg("rating_avg_5_prior"), 2).alias("avg_rating_avg_5_prior"),
        F.percentile_approx("running_sum_rating_share", [0.25, 0.5, 0.75]).alias("running_sum_rating_share_quartiles"),
    )
    .orderBy("avg_rating_avg_5_prior")
)
