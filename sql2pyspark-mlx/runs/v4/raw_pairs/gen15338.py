from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city")
w4 = Window.partitionBy("country", "city").orderBy(F.col("product_launch_date").desc(), "id")
w_part = Window.orderBy(F.col("rating").asc_nulls_last())
w6 = Window.partitionBy("country").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("country").orderBy(F.col("product_launch_date").desc(), "id")

with_latest = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.launch_date").alias("product_launch_date"),
        F.col("c.cost").alias("product_cost"),
    )
)

metrics = (
    with_latest
    .withColumn("running_rating", F.sum("rating").over(w2))
    .withColumn("rating_share", F.round((F.col("rating") / F.sum("rating").over(w3)), 4))
    .withColumn("event_seq", F.row_number().over(w4))
)

with_windows = (
    metrics
    .select(
        "*",
        F.cume_dist().over(w_part).alias("rating_cume_dist"),
        F.first("rating", ignorenulls=True).over(w6).alias("first_known_rating"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    with_windows
    .groupBy(F.date_trunc("WEEK", "product_launch_date").alias("week_start"), "city")
    .agg(
        F.size(F.collect_set("country")).alias("num_countries"),
        F.min("rating").alias("min_rating"),
        F.count_if(F.col("country") == "US").alias("us_events"),
        F.max(F.when(F.col("country") == "JP", F.col("rating_share"))).alias("jp_max_rating_share"),
        F.percentile_approx("product_cost", [0.25, 0.5, 0.75]).alias("product_cost_quartiles"),
    )
)
