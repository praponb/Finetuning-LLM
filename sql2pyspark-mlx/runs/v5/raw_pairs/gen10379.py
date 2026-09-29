from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city", "region").orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("region").orderBy(F.col("sqft").desc(), "id")
w_part = Window.partitionBy("region").orderBy(F.col("opened_date").desc(), "id")
w4 = Window.orderBy(F.col("opened_date").desc(), "id")
w_hist = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_first())
w6 = Window.partitionBy("region", "city").orderBy(F.col("sqft").desc(), "id")
w7 = Window.orderBy("sqft_bucket_2", "id")
w8 = Window.orderBy("rank_in_group", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("event_seq", "id")

valid = (
    df_stores
    .filter(F.col("region").isNotNull() & F.col("store_name").like("Mal%"))
    .select(
        "*",
        F.date_trunc("year", "opened_date").alias("opened_date_period"),
        F.coalesce("region", F.lit("N/A")).alias("resolved_region"),
    )
)

deduped = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

with_windows = (
    deduped
    .select(
        "id",
        "store_name",
        "opened_date_period",
        "rank_in_group",
        F.ntile(5).over(w2).alias("sqft_bucket"),
        F.row_number().over(w_part).alias("event_seq"),
        F.row_number().over(w4).alias("seq_no"),
        F.percent_rank().over(w_hist).alias("sqft_percent_rank"),
        F.ntile(3).over(w6).alias("sqft_bucket_2"),
    )
)

scored = (
    with_windows
    .select(
        "id",
        "store_name",
        "opened_date_period",
        "sqft_bucket",
        F.row_number().over(w7).alias("seq_no_2"),
        F.last("seq_no", ignorenulls=True).over(w8).alias("last_known_seq_no"),
        F.lag("sqft_bucket", 1).over(w9).alias("prev_sqft_bucket"),
    )
)

result = (
    scored
    .groupBy(
        F.when(F.col("seq_no_2") >= 9, "LARGE").otherwise("SMALL").alias("seq_no_2_tier"),
    )
    .agg(
        ((F.unix_timestamp(F.max("opened_date_period")) - F.unix_timestamp(F.min("opened_date_period"))) / 60).alias("span_minutes"),
        F.percentile_approx("seq_no_2", 0.5).alias("seq_no_2_p50"),
        F.min("opened_date_period").alias("started_at"),
    )
    .orderBy(F.col("seq_no_2_tier").asc_nulls_last())
)
