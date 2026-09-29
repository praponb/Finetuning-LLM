from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("starts_on_plus_months", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("cumulative_discount_pct").desc(), "id")

valid = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-04-12").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.trim("channel").alias("clean_channel"),
        F.add_months("starts_on", 12).alias("starts_on_plus_months"),
    )
)

scored = (
    valid
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "clean_channel",
        F.sum("discount_pct").over(w).alias("cumulative_discount_pct"),
        F.sum("discount_pct").over(w2).alias("cumulative_discount_pct_2"),
    )
)

filtered = (
    scored
    .filter((F.col("ends_on") > F.to_date(F.lit("2023-08-18"))) & (F.col("promo_code") == "SPRING10"))
    .select(
        "id",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        "clean_channel",
        "cumulative_discount_pct",
        F.floor("discount_pct").alias("discount_pct_whole"),
    )
)

ranked = (
    filtered
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "promo_code", "starts_on", "discount_pct")
)

result = (
    ranked
    .groupBy(F.date_trunc("YEAR", "starts_on").alias("year_start"))
    .agg(
        F.percentile_approx("discount_pct", 0.95).alias("discount_pct_p95"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
    )
    .filter(F.col("num_promo_codes") >= 1)
    .orderBy(F.col("discount_pct_p95").desc())
)
