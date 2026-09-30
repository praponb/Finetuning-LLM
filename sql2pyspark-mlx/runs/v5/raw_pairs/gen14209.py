from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date_month_end", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_rounded").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("delivered_date") > F.to_date(F.lit("2023-07-17")))
    .withColumn("days_delivered_date_to_shipped_date", F.datediff("shipped_date", "delivered_date"))
    .withColumn("cost_rounded", F.round("cost", 1))
)

enriched = (
    cleaned
    .alias("s")
    .join(df_orders.alias("o"), F.col("s.order_id") == F.col("o.id"), "left")
    .select("s.*", F.col("o.discount"))
)

cleaned_shipments = (
    enriched
    .filter(F.col("delivered_date") > F.lit("2023-04-21").cast("date"))
    .select(
        "*",
        F.datediff("shipped_date", "delivered_date").alias("days_delivered_date_to_shipped_date_2"),
    )
)

valid = (
    cleaned_shipments
    .filter((F.col("shipped_date") >= F.to_date(F.lit("2023-08-24"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "days_delivered_date_to_shipped_date",
        "cost_rounded",
        "discount",
        "days_delivered_date_to_shipped_date_2",
        F.last_day("shipped_date").alias("shipped_date_month_end"),
        F.initcap(F.trim("carrier")).alias("carrier_title"),
    )
)

metrics = (
    valid
    .filter(F.col("delivered_date") <= "2024-07-01")
    .select(
        "id",
        "shipped_date",
        "days_delivered_date_to_shipped_date",
        "cost_rounded",
        "days_delivered_date_to_shipped_date_2",
        "carrier_title",
        F.lag("days_delivered_date_to_shipped_date_2").over(w).alias("prev_days_delivered_date_to_shipped_date_2"),
        F.lead("discount").over(w).alias("next_discount"),
        F.lag("discount").over(w).alias("prev_discount"),
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("event_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("shipped_date") >= F.lit("2023-08-02").cast("date"))
    .select(
        "*",
        (F.col("cost_rounded") + F.col("row_seq")).alias("cost_rounded_plus_row_seq"),
    )
)

remaining_keys = (
    filtered
    .select("id")
    .subtract(
        df_shipments
        .filter(F.col("delivered_date").between("2023-12-01", "2024-02-01"))
        .select("id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

stats = (
    kept_rows
    .groupBy(F.date_trunc("QUARTER", "shipped_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("event_seq", 0.95).alias("event_seq_p95"),
        F.max("days_delivered_date_to_shipped_date").alias("max_days_delivered_date_to_shipped_date"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("days_delivered_date_to_shipped_date"), 2).alias("avg_days_delivered_date_to_shipped_date"),
    )
    .filter(F.col("event_seq_p95") < 1000000)
)

result = (
    stats
    .select(
        "quarter_start",
        "row_count",
        "event_seq_p95",
        "max_days_delivered_date_to_shipped_date",
        "avg_days_delivered_date_to_shipped_date",
    )
)
