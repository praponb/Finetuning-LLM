from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date_plus_months", "id")
w3 = Window.partitionBy("carrier", "order_id").orderBy("delivered_date", "id")
w4 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w5 = Window.partitionBy("carrier", "order_id").orderBy("event_seq", "id")
w6 = Window.orderBy("event_seq", "id").rowsBetween(-5, -1)

cleaned = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-04-19").cast("date")) & (F.col("shipped_date") > "2023-08-01"))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        F.add_months("shipped_date", 12).alias("shipped_date_plus_months"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
        F.row_number().over(w3).alias("seq_no"),
    )
)

latest = (
    with_windows
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "shipped_date_plus_months",
        "event_seq",
        "seq_no",
    )
)

scored = (
    latest
    .filter(F.col("seq_no").isNotNull())
    .select(
        "id",
        "seq_no",
        F.lead("event_seq", 2).over(w5).alias("next_event_seq"),
        F.avg("seq_no").over(w6).alias("seq_no_avg_5_prior"),
    )
)

daily = (
    scored
    .groupBy(
        F.when(F.col("seq_no") >= 8, "PREMIUM").otherwise("STANDARD").alias("seq_no_tier"),
        "seq_no",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.min("seq_no_avg_5_prior").alias("min_seq_no_avg_5_prior"),
        F.count("*").alias("cnt"),
        F.percentile_approx("seq_no", 0.95).alias("seq_no_p95"),
        F.countDistinct("id").alias("distinct_ids_2"),
        F.countDistinct("id").alias("distinct_ids_3"),
    )
)

result = (
    daily
    .select(
        "seq_no_tier",
        "seq_no",
        "distinct_ids",
        "min_seq_no_avg_5_prior",
        "distinct_ids_3",
    )
)
