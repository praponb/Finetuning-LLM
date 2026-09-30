from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city").orderBy("rating", "id")
w3 = Window.partitionBy("country").orderBy(F.col("running_rating").desc())
w_ordered = Window.orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("country").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist = Window.partitionBy("country").orderBy(F.col("next_rating").desc(), "id")
w7 = Window.partitionBy("country").orderBy("next_rating_times_running_rating", "id")

eligible = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.length(F.col("x.product_name")) < 9), "left_anti")
)

with_windows = (
    eligible
    .select(
        "id",
        "supplier_name",
        "country",
        F.sum("rating").over(w).alias("running_rating"),
        F.lead("rating", 2, 0).over(w2).alias("next_rating"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.array_contains(F.col("c.tags"), "premium")), "left_semi")
)

latest = (
    not_flagged
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

kept = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")) & (F.col("c.supplier_id").isin([8, 12])), "left_semi")
)

valid = (
    kept
    .filter(F.lower(F.trim("supplier_name")) == "lion city imports")
    .select(
        "id",
        "supplier_name",
        "country",
        "next_rating",
        "row_num",
        (F.col("next_rating") * F.col("running_rating")).alias("next_rating_times_running_rating"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.last("next_rating_times_running_rating", ignorenulls=True).over(w_ordered).alias("last_known_next_rating_times_running_rating"),
        F.last("next_rating_times_running_rating", ignorenulls=True).over(w5).alias("last_known_next_rating_times_running_rating_2"),
        F.ntile(3).over(w_hist).alias("next_rating_bucket"),
        F.row_number().over(w_hist).alias("seq_no"),
        F.ntile(3).over(w7).alias("next_rating_times_running_rating_bucket"),
    )
)

totals = (
    scored
    .groupBy(
        F.when(F.col("last_known_next_rating_times_running_rating") >= 75, "LARGE").otherwise("SMALL").alias("last_known_next_rating_times_running_rating_tier"),
        "seq_no",
        F.when(F.col("next_rating_bucket") >= 8, "PREMIUM").otherwise("STANDARD").alias("next_rating_bucket_tier"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.sort_array(F.collect_set("country")).alias("country_list"),
        F.round(F.avg("seq_no"), 2).alias("avg_seq_no"),
    )
    .filter(F.col("distinct_ids") >= 1)
)

result = (
    totals
    .select(
        "last_known_next_rating_times_running_rating_tier",
        "seq_no",
        "next_rating_bucket_tier",
        "distinct_ids",
        "country_list",
        "avg_seq_no",
    )
    .orderBy(F.col("avg_seq_no").desc(), "next_rating_bucket_tier")
)
