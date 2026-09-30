from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("starts_on", "id")
w2 = Window.orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("ends_on", "id")

filtered = (
    df_promotions
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        F.date_format("ends_on", "MMM yyyy").alias("ends_on_label"),
    )
)

ranked = (
    filtered
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

base = (
    ranked
    .filter(F.month("starts_on").isin([1, 6, 9]))
    .withColumn("channel_promo_code", F.concat("channel", F.lit(" - "), "promo_code"))
    .withColumn("resolved_channel", F.coalesce("channel", F.lit("OTHER")))
)

scored = (
    base
    .select(
        "*",
        F.last("channel", ignorenulls=True).over(w2).alias("last_known_channel"),
    )
)

metrics = (
    scored
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "ends_on_label",
        F.row_number().over(w3).alias("seq_no"),
    )
)

result = (
    metrics
    .groupBy("seq_no")
    .agg(
        F.min("seq_no").alias("min_seq_no"),
        F.count(F.when(F.col("channel") == "web", 1)).alias("web_rows"),
    )
    .orderBy(F.col("min_seq_no").asc_nulls_last())
)
