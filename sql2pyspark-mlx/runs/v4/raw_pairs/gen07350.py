from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("discount_pct_int_2", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.partitionBy("promo_code").orderBy("starts_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_int_2").desc_nulls_first(), "id")
w_recent = Window.orderBy(F.col("first_known_discount_pct_int").asc_nulls_last())
w5 = Window.partitionBy("promo_code").orderBy("running_discount_pct_int_2", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.partitionBy("promo_code")
w7 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.orderBy("promo_code_avg_running_discount_pct_int_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.partitionBy("promo_code").orderBy("first_known_discount_pct_int_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("channel").isNotNull() & (F.col("discount_pct") >= 0.19))
    .withColumn("promo_code_channel", F.concat("promo_code", F.lit(" "), "channel"))
    .withColumn("promo_code_length", F.length("promo_code"))
)

valid = (
    base
    .filter(F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "promo_code_length",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
        F.col("discount_pct").cast("int").alias("discount_pct_int_2"),
        F.coalesce("channel", F.lit("OTHER")).alias("channel_filled"),
    )
)

cleaned = (
    valid
    .filter(F.col("promo_code") == "WELCOME5")
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "discount_pct_int",
        "discount_pct_int_2",
        F.add_months("starts_on", 1).alias("starts_on_plus_months"),
        F.coalesce("discount_pct_int_2", F.lit(0)).alias("discount_pct_int_2_filled"),
    )
)

with_windows = (
    cleaned
    .select(
        "*",
        F.avg("discount_pct_int").over(w).alias("discount_pct_int_avg_5"),
        F.sum("discount_pct_int_2").over(w2).alias("running_discount_pct_int_2"),
        F.first("discount_pct_int", ignorenulls=True).over(w2).alias("first_known_discount_pct_int"),
    )
)

deduped = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

with_windows_promotions = (
    deduped
    .select(
        "*",
        F.rank().over(w_recent).alias("first_known_discount_pct_int_rank"),
        F.avg("discount_pct_int").over(w5).alias("discount_pct_int_avg_6"),
        F.avg("running_discount_pct_int_2").over(w6).alias("promo_code_avg_running_discount_pct_int_2"),
        F.sum("discount_pct_int_avg_5").over(w7).alias("cumulative_discount_pct_int_avg_5"),
    )
)

enriched = (
    with_windows_promotions
    .select(
        "*",
        F.last("discount_pct_int", ignorenulls=True).over(w8).alias("last_known_discount_pct_int"),
        F.last("running_discount_pct_int_2", ignorenulls=True).over(w9).alias("last_known_running_discount_pct_int_2"),
    )
)

result = (
    enriched
    .select(
        "id",
        "promo_code",
        "starts_on",
        "discount_pct",
        "starts_on_plus_months",
        "discount_pct_int_avg_5",
        "latest_rank",
        "discount_pct_int_avg_6",
        "promo_code_avg_running_discount_pct_int_2",
    )
    .orderBy("starts_on_plus_months")
)
