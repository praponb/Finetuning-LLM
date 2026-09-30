from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy(F.col("starts_on").desc(), "id")
w2 = Window.orderBy("starts_on", "id")
w3 = Window.orderBy(F.col("starts_on").desc(), "id")
w4 = Window.orderBy(F.col("adjusted_event_seq").desc(), "id")
w5 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("starts_on", "id").rowsBetween(-3, Window.currentRow)

valid = (
    df_promotions
    .filter(F.col("ends_on") > F.lit("2023-07-10").cast("date"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "starts_on")
)

enriched = (
    ranked
    .select(
        "id",
        "starts_on",
        F.row_number().over(w2).alias("seq_no"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

valid_promotions = (
    enriched
    .filter(F.col("event_seq") > 6)
    .withColumn("starts_on_plus_67d", F.date_add("starts_on", 67))
)

cleaned = (
    valid_promotions
    .filter(F.col("seq_no") >= 5)
    .select(
        "id",
        "starts_on",
        "seq_no",
        "event_seq",
        "starts_on_plus_67d",
        (F.col("seq_no") + F.col("event_seq")).alias("seq_no_plus_event_seq"),
        F.round((F.col("event_seq") * 1.15), 2).alias("adjusted_event_seq"),
    )
)

enriched_promotions = (
    cleaned
    .withColumn("adjusted_event_seq_bucket", F.ntile(3).over(w4))
    .withColumn("last_known_seq_no", F.last("seq_no", ignorenulls=True).over(w5))
    .withColumn("seq_no_plus_event_seq_avg_3", F.avg("seq_no_plus_event_seq").over(w6))
)

cleaned_promotions = (
    enriched_promotions
    .filter(F.col("adjusted_event_seq_bucket").between(6, 8))
    .withColumn("days_starts_on_plus_67d_to_starts_on", F.datediff("starts_on", "starts_on_plus_67d"))
    .withColumn("event_seq_plus_seq_no_plus_event_seq", (F.col("event_seq") + F.col("seq_no_plus_event_seq")))
)

result = (
    cleaned_promotions
    .select(
        "id",
        "event_seq_plus_seq_no_plus_event_seq",
        "seq_no_plus_event_seq",
        "adjusted_event_seq",
    )
    .unpivot(
        ids=["id"],
        values=["event_seq_plus_seq_no_plus_event_seq", "seq_no_plus_event_seq", "adjusted_event_seq"],
        variableColumnName="metric",
        valueColumnName="value",
    )
    .filter(F.col("value").isNotNull())
)
