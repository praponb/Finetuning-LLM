from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.orderBy("discount_pct", "id")
w3 = Window.partitionBy("promo_code")
w4 = Window.orderBy(F.col("discount_pct").desc(), "id")
w5 = Window.partitionBy("promo_code").orderBy(F.col("ends_on").desc(), "id")

base = (
    df_promotions
    .filter((F.col("ends_on") >= F.lit("2024-01-15").cast("date")) & F.col("channel").isNotNull() & F.month("starts_on").isin([5, 6, 11]))
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.26, "High").when(F.col("discount_pct") >= 0.13, "Medium").otherwise("Low").alias("discount_pct_level"),
        F.when(F.col("discount_pct") >= 0.27, "High").when(F.col("discount_pct") >= 0.16, "Medium").otherwise("Low").alias("discount_pct_band"),
    )
)

base_promotions = (
    base
    .filter((F.col("starts_on") >= F.lit("2023-11-20").cast("date")) & F.col("channel").isNotNull() & (F.col("channel") == "mobile"))
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "discount_pct_level",
        F.date_format("starts_on", "MMM yyyy").alias("starts_on_label"),
        F.add_months("starts_on", 1).alias("starts_on_plus_months"),
    )
)

with_windows = (
    base_promotions
    .withColumn("discount_pct_bucket", F.ntile(4).over(w))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 1).over(w2))
    .withColumn("discount_pct_share", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("discount_pct_share_2", F.round((F.col("discount_pct") / F.sum("discount_pct").over(w3)), 4))
)

with_windows_promotions = (
    with_windows
    .withColumn("prev_discount_pct_2", F.lag("discount_pct", 1).over(w))
    .withColumn("promo_code_max_discount_pct_share", F.max("discount_pct_share").over(w3))
    .withColumn("prev_discount_pct_share_2", F.lag("discount_pct_share_2", 2).over(w2))
)

deduped = (
    with_windows_promotions
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

result = (
    deduped
    .select(
        "id",
        "promo_code",
        "ends_on",
        "discount_pct",
        "starts_on_label",
        "discount_pct_bucket",
        "prev_discount_pct",
        "row_seq",
        "prev_discount_pct_2",
    )
)
