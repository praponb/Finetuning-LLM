from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("channel").orderBy("discount_pct_rounded", "id")
w3 = Window.partitionBy("channel", "promo_code").orderBy("next_discount_pct_rounded_2")
w4 = Window.orderBy("discount_pct_rounded_2", "id")
w5 = Window.partitionBy("promo_code", "channel").orderBy("discount_pct_rounded_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_promotions
    .filter(F.col("starts_on") >= "2023-03-03")
    .withColumn("promo_code_group", F.when(F.col("promo_code").isin(["BUNDLE", "VIP"]), "Priority").otherwise("Other"))
)

filtered = (
    base
    .filter((F.col("starts_on") >= F.lit("2023-07-19").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "promo_code_group",
        F.round("discount_pct", 1).alias("discount_pct_rounded"),
        F.round("discount_pct", 1).alias("discount_pct_rounded_2"),
        F.coalesce("channel", F.lit("N/A")).alias("resolved_channel"),
    )
)

with_windows = (
    filtered
    .filter(F.col("channel").isin(["partner", "web", "store"]))
    .withColumn("prev_discount_pct_rounded", F.lag("discount_pct_rounded").over(w))
    .withColumn("next_discount_pct_rounded_2", F.lead("discount_pct_rounded_2").over(w))
    .withColumn("row_seq", F.row_number().over(w))
    .withColumn("prev_discount_pct_rounded_2", F.lag("discount_pct_rounded_2", 1).over(w2))
)

with_windows_promotions = (
    with_windows
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "promo_code_group",
        "discount_pct_rounded_2",
        "prev_discount_pct_rounded",
        "next_discount_pct_rounded_2",
        "row_seq",
        "prev_discount_pct_rounded_2",
        F.rank().over(w3).alias("next_discount_pct_rounded_2_rank"),
        F.ntile(4).over(w4).alias("discount_pct_rounded_2_bucket"),
        F.last("row_seq", ignorenulls=True).over(w5).alias("last_known_row_seq"),
    )
)

result = (
    with_windows_promotions
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "next_discount_pct_rounded_2",
        "discount_pct_rounded_2_bucket",
        "last_known_row_seq",
    )
    .orderBy("ends_on")
)
