from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct").desc())
w3 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("promo_code").orderBy(F.col("discount_pct_whole").desc(), "id")
w5 = Window.orderBy("discount_pct_int", "id")

base = (
    df_promotions
    .filter((F.col("starts_on") > F.lit("2023-10-11").cast("date")) & F.col("channel").isNotNull() & F.month("starts_on").isin([1, 3, 6]))
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.date_format("starts_on", "yyyy-MM").alias("starts_on_label"),
    )
)

deduped = (
    base
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
)

latest = (
    deduped
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

base_promotions = (
    latest
    .filter(F.col("discount_pct") > 0.21)
    .select(
        "id",
        "promo_code",
        "discount_pct",
        "starts_on_label",
        "rn",
        F.floor("discount_pct").alias("discount_pct_whole"),
        F.col("discount_pct").cast("int").alias("discount_pct_int"),
    )
)

base_promotions_2 = (
    base_promotions
    .filter(F.col("discount_pct") >= 0.19)
    .select("*", F.upper("promo_code").alias("promo_code_upper"))
)

with_windows = (
    base_promotions_2
    .withColumn("running_discount_pct_int", F.sum("discount_pct_int").over(w3))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("next_discount_pct_whole", F.lead("discount_pct_whole", 1).over(w5))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("running_discount_pct_int") >= 442, "LARGE").otherwise("SMALL").alias("running_discount_pct_int_tier"),
    )
    .agg(
        F.stddev_samp("discount_pct").alias("discount_pct_stddev"),
        F.count("*").alias("num_rows"),
        F.round(F.avg("next_discount_pct_whole"), 2).alias("avg_next_discount_pct_whole"),
        F.percentile_approx("row_seq", [0.25, 0.5, 0.75]).alias("row_seq_quartiles"),
    )
    .orderBy(F.col("num_rows").asc_nulls_first())
)
