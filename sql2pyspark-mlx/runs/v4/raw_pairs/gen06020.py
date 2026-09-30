from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("promo_code", "channel").orderBy("discount_pct", "id")
w_hist_2 = Window.orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct").desc(), "id")
w4 = Window.partitionBy("channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("ends_on", "id")
w6 = Window.partitionBy("channel", "promo_code").orderBy(F.col("discount_pct_bucket").desc(), "id")
w7 = Window.orderBy(F.col("days_since_starts_on").desc_nulls_first())
w8 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("promo_code", "channel").orderBy("starts_on", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter((F.col("starts_on") > F.to_date(F.lit("2023-09-27"))) & F.col("channel").isNotNull() & (F.col("channel") == "web"))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.datediff(F.current_date(), "starts_on").alias("days_since_starts_on"),
        F.upper("promo_code").alias("promo_code_upper"),
        F.coalesce("channel", F.lit("UNKNOWN")).alias("channel_clean"),
    )
)

enriched = (
    base
    .select(
        "*",
        F.row_number().over(w_hist).alias("row_seq"),
        F.dense_rank().over(w_hist_2).alias("discount_pct_dense_rank"),
        F.ntile(4).over(w3).alias("discount_pct_bucket"),
        F.first("channel", ignorenulls=True).over(w4).alias("first_known_channel"),
    )
)

metrics = (
    enriched
    .select(
        "*",
        F.row_number().over(w5).alias("row_seq_2"),
        F.ntile(10).over(w6).alias("discount_pct_bucket_bucket"),
        F.dense_rank().over(w7).alias("days_since_starts_on_dense_rank"),
        F.first("discount_pct_bucket", ignorenulls=True).over(w8).alias("first_known_discount_pct_bucket"),
        F.last("days_since_starts_on", ignorenulls=True).over(w9).alias("last_known_days_since_starts_on"),
    )
)

result = (
    metrics
    .groupBy("channel")
    .pivot("promo_code", ["BUNDLE", "WELCOME5"])
    .agg(F.sum("days_since_starts_on_dense_rank"))
    .select(
        "channel",
        F.col("BUNDLE").alias("bundle"),
        F.col("WELCOME5").alias("welcome5"),
    )
)
