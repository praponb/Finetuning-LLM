from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("city", "country").orderBy(F.col("rating").desc(), "id")
w3 = Window.partitionBy("country", "city").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w6 = Window.partitionBy("country").orderBy("cumulative_rating", "id")
w7 = Window.partitionBy("country")
w8 = Window.partitionBy("city")
w9 = Window.orderBy(F.col("seq_no_share_2").desc_nulls_first())

enriched = (
    df_suppliers
    .filter(F.length("supplier_name") > 7)
    .withColumn("cumulative_rating", F.sum("rating").over(w))
    .withColumn("row_seq", F.row_number().over(w2))
    .withColumn("running_rating", F.sum("rating").over(w3))
    .withColumn("running_rating_2", F.sum("rating").over(w4))
)

with_windows = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("event_seq"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

remaining_keys = (
    with_windows
    .select("country", "city")
    .subtract(
        df_suppliers
        .filter(F.col("country") == "UK")
        .select("country", "city")
    )
)

kept_rows = (
    with_windows
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.country") == F.col("k.country")) & (F.col("s.city") == F.col("k.city")), "inner")
    .select("s.*")
)

with_windows_suppliers = (
    kept_rows
    .select(
        "id",
        "city",
        "rating",
        "cumulative_rating",
        "row_seq",
        "running_rating",
        "event_seq",
        "seq_no",
        F.round((F.col("cumulative_rating") / F.sum("cumulative_rating").over(w7)), 4).alias("cumulative_rating_share"),
    )
)

not_flagged = (
    with_windows_suppliers
    .alias("s")
    .join(df_products.alias("ch"), (F.col("ch.supplier_id") == F.col("s.id")) & (F.col("ch.supplier_id").isNotNull()), "left_anti")
)

with_windows_suppliers_2 = (
    not_flagged
    .select(
        "rating",
        "running_rating",
        "event_seq",
        "cumulative_rating_share",
        F.round((F.col("rating") / F.sum("rating").over(w8)), 4).alias("rating_share"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w8)), 4).alias("seq_no_share"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w8)), 4).alias("seq_no_share_2"),
        F.round((F.col("seq_no") / F.sum("seq_no").over(w8)), 4).alias("seq_no_share_3"),
        F.avg("running_rating").over(w8).alias("city_avg_running_rating"),
    )
)

with_windows_suppliers_3 = (
    with_windows_suppliers_2
    .withColumn("seq_no_share_2_percent_rank", F.percent_rank().over(w9))
)

result = (
    with_windows_suppliers_3
    .groupBy(
        F.when(F.col("seq_no_share") >= 0.63, "PREMIUM").otherwise("STANDARD").alias("seq_no_share_tier"),
    )
    .agg(
        F.percentile_approx("rating_share", 0.95).alias("rating_share_p95"),
        F.round(F.avg("city_avg_running_rating"), 2).alias("avg_city_avg_running_rating"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.round(F.avg("seq_no_share_3"), 2).alias("avg_seq_no_share_3"),
    )
    .filter(F.col("rating_p95") >= 0)
)
