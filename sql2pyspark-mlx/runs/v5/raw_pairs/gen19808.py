from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country")
w3 = Window.partitionBy("country", "city").orderBy("rating_filled", "id")
w4 = Window.partitionBy("country").orderBy("cost", "id")
w5 = Window.partitionBy("country", "city")
w6 = Window.partitionBy("country").orderBy("cost_share", "id")
w7 = Window.partitionBy("country").orderBy(F.col("cost_share").asc_nulls_last())
w_hist = Window.partitionBy("city").orderBy("country_min_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cost_share_percent_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_suppliers
    .filter(F.col("city").isin(["Berlin", "Tokyo"]))
    .select(
        "id",
        "country",
        "city",
        F.coalesce("rating", F.lit(0)).alias("rating_filled"),
    )
)

with_latest = (
    cleaned
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.cost"))
)

scored = (
    with_latest
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w2)), 4))
    .withColumn("rating_filled_bucket", F.ntile(10).over(w3))
    .withColumn("next_rating_filled", F.lead("rating_filled", 1).over(w4))
)

with_windows = (
    scored
    .filter(F.col("rating_filled") < 38)
    .select(
        "id",
        "city",
        "cost",
        "rating_filled_bucket",
        F.min("cost").over(w5).alias("country_min_cost"),
        F.lead("cost", 2).over(w6).alias("next_cost"),
        F.percent_rank().over(w7).alias("cost_share_percent_rank"),
    )
)

scored_suppliers = (
    with_windows
    .select(
        "*",
        F.max("rating_filled_bucket").over(w_hist).alias("running_max_rating_filled_bucket"),
        F.count("rating_filled_bucket").over(w_hist).alias("running_count_rating_filled_bucket"),
        F.sum("rating_filled_bucket").over(w_hist).alias("running_sum_rating_filled_bucket"),
        F.last("rating_filled_bucket", ignorenulls=True).over(w9).alias("last_known_rating_filled_bucket"),
    )
)

not_flagged = (
    scored_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") != "Globex"), "left_semi")
)

result = (
    not_flagged
    .groupBy("city")
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("next_cost")).otherwise(0)).alias("new_york_next_cost"),
        F.percentile_approx("country_min_cost", [0.25, 0.5, 0.75]).alias("country_min_cost_quartiles"),
        F.avg("running_sum_rating_filled_bucket").alias("avg_running_sum_rating_filled_bucket"),
        F.sum(F.when(F.col("city") == "Singapore", F.col("cost_share_percent_rank")).otherwise(0)).alias("singapore_cost_share_percent_rank"),
    )
    .filter(F.col("singapore_cost_share_percent_rank") >= 0)
    .orderBy(F.col("city").desc())
)
