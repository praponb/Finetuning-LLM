from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_ordered = Window.partitionBy("promo_code")
w_ordered_2 = Window.partitionBy("channel").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_recent = Window.partitionBy("channel").orderBy("ends_on_period", "id")
w4 = Window.partitionBy("promo_code").orderBy(F.col("seq_no").desc(), "id")
w5 = Window.partitionBy("channel")
w6 = Window.partitionBy("channel").orderBy("ends_on_period", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_promotions
    .filter((F.col("ends_on") > F.lit("2024-04-04").cast("date")) & F.col("channel").isNotNull() & ~F.col("promo_code").isin(["BUNDLE", "WELCOME5"]))
    .select(
        "id",
        "channel",
        "promo_code",
        "ends_on",
        "discount_pct",
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

base = (
    filtered
    .filter(F.col("channel").isNotNull())
    .select("*", F.date_trunc("WEEK", "ends_on").alias("ends_on_period"))
)

metrics = (
    base
    .select(
        "*",
        F.count("discount_pct_int").over(w_ordered).alias("promo_code_count_discount_pct_int"),
        F.sum("discount_pct_int").over(w_ordered_2).alias("cumulative_discount_pct_int"),
        F.row_number().over(w_recent).alias("seq_no"),
    )
)

result = (
    metrics
    .withColumn("seq_no_2", F.row_number().over(w4))
    .withColumn("channel_avg_discount_pct_int", F.avg("discount_pct_int").over(w5))
    .withColumn("cumulative_cumulative_discount_pct_int", F.sum("cumulative_discount_pct_int").over(w6))
)
