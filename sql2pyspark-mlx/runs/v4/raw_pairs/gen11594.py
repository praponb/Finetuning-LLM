from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("opened_date", "id")
w2 = Window.orderBy("opened_date_minus_34d", "id")
w3 = Window.orderBy(F.col("opened_date_minus_34d").desc(), "id")
w4 = Window.orderBy(F.col("seq_no").desc(), "id")
w5 = Window.orderBy(F.col("seq_no_2").desc(), "id")
w6 = Window.orderBy("seq_no_2", "id").rowsBetween(-5, Window.currentRow)
w7 = Window.orderBy("seq_no_2_avg_5", "id")
w8 = Window.orderBy(F.col("seq_no_2_avg_5").desc(), "id")
w9 = Window.orderBy("seq_no_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.lit("2023-03-25").cast("date")) & (F.col("sqft") > 12888))
    .select(
        "id",
        "store_name",
        "opened_date",
        F.date_sub("opened_date", 34).alias("opened_date_minus_34d"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.row_number().over(w2).alias("seq_no_2"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

with_windows = (
    enriched
    .withColumn("seq_no_bucket", F.ntile(4).over(w4))
    .withColumn("seq_no_2_bucket", F.ntile(3).over(w5))
    .withColumn("seq_no_2_avg_5", F.avg("seq_no_2").over(w6))
)

enriched_stores = (
    with_windows
    .select(
        "*",
        F.ntile(4).over(w7).alias("seq_no_2_avg_5_bucket"),
        F.ntile(4).over(w8).alias("seq_no_2_avg_5_bucket_2"),
        F.last("seq_no", ignorenulls=True).over(w9).alias("last_known_seq_no"),
        F.row_number().over(w4).alias("row_seq_2"),
        F.last("seq_no_2_bucket", ignorenulls=True).over(w_recent).alias("last_known_seq_no_2_bucket"),
    )
)

result = (
    enriched_stores
    .groupBy(F.date_trunc("YEAR", "opened_date_minus_34d").alias("year_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("last_known_seq_no_2_bucket").alias("min_last_known_seq_no_2_bucket"),
        F.round(F.avg("seq_no_2"), 2).alias("avg_seq_no_2"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.percentile_approx("last_known_seq_no_2_bucket", 0.95).alias("last_known_seq_no_2_bucket_p95"),
    )
    .orderBy(F.col("distinct_ids").asc_nulls_last())
)
