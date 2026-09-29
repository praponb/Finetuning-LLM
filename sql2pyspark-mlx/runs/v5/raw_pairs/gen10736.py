from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("opened_date").desc(), "id")
w2 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("sqft", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("city").orderBy("sqft_avg_6_prior", "id")
w_grp = Window.partitionBy("city", "region").orderBy("seq_no", "id")
w_part = Window.partitionBy("city").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

metrics = (
    df_stores
    .select(
        "id",
        "city",
        "region",
        "opened_date",
        "sqft",
        F.row_number().over(w).alias("seq_no"),
        F.sum("sqft").over(w2).alias("running_sqft"),
        F.first("region", ignorenulls=True).over(w3).alias("first_known_region"),
        F.avg("sqft").over(w4).alias("sqft_avg_6_prior"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "city",
        "opened_date",
        "sqft",
        "seq_no",
        "sqft_avg_6_prior",
        F.row_number().over(w5).alias("event_seq"),
        F.lag("seq_no", 2).over(w_grp).alias("prev_seq_no"),
    )
)

scored = (
    with_windows
    .filter(F.col("sqft_avg_6_prior").between(9639, 10943))
    .select(
        "id",
        "event_seq",
        "prev_seq_no",
        F.count("sqft").over(w_part).alias("running_count_sqft"),
        F.min("sqft").over(w_part).alias("running_min_sqft"),
        F.max("sqft").over(w_part).alias("running_max_sqft"),
        F.last("sqft", ignorenulls=True).over(w_part).alias("last_known_sqft"),
    )
)

filtered = (
    scored
    .filter(F.col("prev_seq_no").isNotNull())
    .select(
        "*",
        (F.col("running_count_sqft") + F.col("prev_seq_no")).alias("running_count_sqft_plus_prev_seq_no"),
        F.col("running_max_sqft").cast("int").alias("running_max_sqft_int"),
    )
)

result = (
    filtered
    .groupBy(
        F.when(F.col("event_seq") >= 7, "PREMIUM").otherwise("STANDARD").alias("event_seq_tier"),
        F.when(F.col("running_count_sqft") >= 15, "PREMIUM").otherwise("STANDARD").alias("running_count_sqft_tier"),
        F.when(F.col("running_max_sqft_int") >= 63, "LARGE").otherwise("SMALL").alias("running_max_sqft_int_tier"),
    )
    .agg(
        F.percentile_approx("running_max_sqft", [0.25, 0.5, 0.75]).alias("running_max_sqft_quartiles"),
        F.round(F.avg("running_min_sqft"), 2).alias("avg_running_min_sqft"),
    )
)
