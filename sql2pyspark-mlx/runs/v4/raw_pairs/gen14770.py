from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("channel", "promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("channel", "promo_code").orderBy("ends_on", "id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-06-15").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "channel",
        "promo_code",
        "starts_on",
        "ends_on",
        "discount_pct",
        F.substring("channel", 1, 2).alias("channel_prefix"),
    )
)

filtered = (
    base
    .filter((F.col("starts_on") > F.lit("2024-03-03").cast("date")) & F.col("channel").isNotNull() & (F.col("channel") == "web"))
    .withColumn("channel_promo_code", F.concat("channel", F.lit(" - "), "promo_code"))
)

metrics = (
    filtered
    .withColumn("cumulative_discount_pct", F.sum("discount_pct").over(w))
    .withColumn("prev_discount_pct", F.lag("discount_pct", 2).over(w2))
)

base_promotions = (
    metrics
    .filter(F.col("ends_on") <= "2025-05-29")
    .select(
        "id",
        "channel",
        "starts_on",
        "ends_on",
        "discount_pct",
        "channel_prefix",
        "channel_promo_code",
        "prev_discount_pct",
        F.col("cumulative_discount_pct").cast("double").alias("cumulative_discount_pct_num"),
    )
)

stats = (
    base_promotions
    .groupBy(F.date_trunc("YEAR", "starts_on").alias("year_start"))
    .agg(
        F.count("*").alias("cnt"),
        F.sort_array(F.collect_set("channel")).alias("channel_list"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount_pct")).otherwise(0)).alias("store_discount_pct"),
        F.sort_array(F.collect_set("channel")).alias("channel_list_2"),
        F.sum(F.when(F.col("channel") == "store", F.col("discount_pct"))).alias("store_total_discount_pct"),
    )
    .filter(F.col("cnt") < 1)
)

result = (
    stats
    .select("year_start", "channel_list", "channel_list_2", "store_total_discount_pct")
)
