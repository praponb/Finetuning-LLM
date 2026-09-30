from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel").orderBy("ends_on_period", "id")
w2 = Window.partitionBy("channel").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("channel").orderBy(F.col("session_no").desc(), "id")

cleaned = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("ends_on") >= F.to_date(F.lit("2023-08-18"))))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.date_trunc("MONTH", "ends_on").alias("ends_on_period"),
        F.datediff(F.current_date(), "ends_on").alias("days_since_ends_on"),
    )
)

with_gaps = (
    cleaned
    .withColumn("session_start_flag", F.when(F.col("ends_on_period") - F.lag("ends_on_period").over(w) > F.expr("INTERVAL 7 DAYS"), 1).otherwise(0))
)

islands = with_gaps.withColumn("session_no", F.sum("session_start_flag").over(w2))

filtered = (
    islands
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "discount_pct",
        "days_since_ends_on",
        "session_no",
        (F.col("discount_pct") - F.col("days_since_ends_on")).alias("discount_pct_minus_days_since_ends_on"),
    )
)

latest = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

result = latest.select("id", "session_no", "discount_pct_minus_days_since_ends_on")
