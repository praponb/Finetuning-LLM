from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("rating").desc(), "id")
w2 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_rating", "id")
w4 = Window.orderBy("rating_bucket", "id")
w5 = Window.orderBy("rating", "id").rowsBetween(-5, -1)
w6 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.launch_date").desc(), F.col("c.id").desc())
w7 = Window.orderBy("rating_bucket_bucket", "id")
w8 = Window.orderBy("launch_date", "id")
w9 = Window.orderBy(F.col("next_running_rating").desc())

with_windows = (
    df_suppliers
    .select(
        "id",
        "supplier_name",
        "rating",
        F.ntile(4).over(w).alias("rating_bucket"),
        F.sum("rating").over(w2).alias("running_rating"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "supplier_name",
        "running_rating",
        F.lead("running_rating", 2).over(w3).alias("next_running_rating"),
        F.ntile(10).over(w4).alias("rating_bucket_bucket"),
        F.avg("running_rating").over(w5).alias("running_rating_avg_5_prior"),
    )
)

with_last = (
    enriched
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.launch_date"))
)

enriched_suppliers = (
    with_last
    .filter(F.length("supplier_name") >= 11)
    .select(
        "*",
        F.lead("rating_bucket_bucket", 1).over(w7).alias("next_rating_bucket_bucket"),
        F.lag("running_rating", 2).over(w8).alias("prev_running_rating"),
        F.cume_dist().over(w9).alias("next_running_rating_cume_dist"),
        F.dense_rank().over(w9).alias("next_running_rating_dense_rank"),
    )
)

not_flagged = (
    enriched_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.launch_date") <= F.col("s.launch_date")), "left_anti")
)

not_flagged_suppliers = (
    not_flagged
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.brand") == "Umbrella"), "left_anti")
)

summary = (
    not_flagged_suppliers
    .rollup(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"))
    .agg(
        F.percentile_approx("next_running_rating_dense_rank", [0.25, 0.5, 0.75]).alias("next_running_rating_dense_rank_quartiles"),
        F.sum("next_running_rating_dense_rank").alias("total_next_running_rating_dense_rank"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("next_running_rating", 0.95).alias("next_running_rating_p95"),
    )
)

result = (
    summary
    .select("quarter_start", "next_running_rating_p95")
    .orderBy(F.col("next_running_rating_p95").desc_nulls_last(), "quarter_start")
)
