from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").asc_nulls_last())
w2 = Window.partitionBy("country", "city").orderBy(F.col("row_num").desc(), "id")
w3 = Window.partitionBy("city").orderBy("row_num", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("country", "city").orderBy("rating", "id")
w5 = Window.partitionBy("city").orderBy(F.col("row_num").desc(), "id")
w6 = Window.partitionBy("city", "country").orderBy("row_num", "id")
w7 = Window.partitionBy("country")

deduped = (
    df_suppliers
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

with_windows = (
    deduped
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.first("city", ignorenulls=True).over(w3).alias("first_known_city"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

deduped_suppliers = (
    with_windows
    .withColumn("row_num_2", F.row_number().over(w5))
    .filter(F.col("row_num_2") == 1)
    .select("id", "supplier_name", "country", "city", "rating", "row_num", "seq_no")
)

enriched = (
    deduped_suppliers
    .select(
        "*",
        F.lag("seq_no").over(w6).alias("prev_seq_no"),
        F.lead("rating").over(w6).alias("next_rating"),
        F.lag("rating").over(w6).alias("prev_rating"),
        F.row_number().over(w6).alias("row_seq"),
        F.sum("rating").over(w7).alias("country_sum_rating"),
    )
)

grouped = (
    enriched
    .groupBy(
        F.when(F.col("prev_seq_no") >= 8, "LARGE").otherwise("SMALL").alias("prev_seq_no_tier"),
        "city",
    )
    .agg(
        F.sum(F.when(F.col("city") == "New York", F.col("next_rating")).otherwise(0)).alias("new_york_next_rating"),
        F.count(F.when(F.col("country") == "TH", 1)).alias("th_rows"),
        F.percentile_approx("rating", 0.95).alias("rating_p95"),
        F.count_if(F.col("country") == "UK").alias("uk_events"),
    )
    .filter(F.col("th_rows") > 1)
)

result = (
    grouped
    .select("prev_seq_no_tier", "city", "th_rows", "rating_p95", "uk_events")
    .orderBy(F.col("rating_p95").asc_nulls_first())
)
