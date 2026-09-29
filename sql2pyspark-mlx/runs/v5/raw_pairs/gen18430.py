from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("promo_code").orderBy("discount_pct", "id")
w2 = Window.partitionBy("promo_code").orderBy("discount_pct", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("promo_code").orderBy(F.col("next_discount_pct").desc(), "id")
w4 = Window.partitionBy("promo_code").orderBy("next_discount_pct_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.orderBy("running_discount_pct", "id").rowsBetween(-3, -1)
w6 = Window.orderBy(F.col("next_discount_pct").desc(), "id")

filtered = (
    df_promotions
    .filter((F.col("starts_on") >= F.lit("2023-09-05").cast("date")) & F.col("channel").isNotNull())
    .select(
        "id",
        "promo_code",
        "discount_pct",
        F.when(F.col("discount_pct") >= 0.3, "High").when(F.col("discount_pct") >= 0.13, "Medium").otherwise("Low").alias("discount_pct_level"),
    )
)

enriched = (
    filtered
    .withColumn("next_discount_pct", F.lead("discount_pct").over(w_grp))
    .withColumn("event_seq", F.row_number().over(w_grp))
    .withColumn("running_discount_pct", F.sum("discount_pct").over(w2))
)

valid = (
    enriched
    .filter(F.col("running_discount_pct").between(1, 2))
    .withColumn("next_discount_pct_int", F.col("next_discount_pct").cast("int"))
)

deduped = (
    valid
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    deduped
    .withColumn("first_known_event_seq", F.first("event_seq", ignorenulls=True).over(w4))
    .withColumn("running_discount_pct_avg_3_prior", F.avg("running_discount_pct").over(w5))
    .withColumn("next_discount_pct_bucket", F.ntile(3).over(w6))
)

base = (
    scored
    .filter(F.col("running_discount_pct") < 1)
    .select(
        "*",
        F.round("running_discount_pct_avg_3_prior", 0).alias("running_discount_pct_avg_3_prior_rounded"),
        F.ceil("event_seq").alias("event_seq_whole"),
    )
)

result = (
    base
    .groupBy(
        F.when(F.col("first_known_event_seq") >= 8, "LARGE").otherwise("SMALL").alias("first_known_event_seq_tier"),
    )
    .agg(
        F.percentile_approx("running_discount_pct_avg_3_prior_rounded", 0.95).alias("running_discount_pct_avg_3_prior_rounded_p95"),
        F.size(F.collect_set("promo_code")).alias("num_promo_codes"),
        F.count("*").alias("event_count"),
    )
    .filter(F.col("event_count") < 1000)
    .orderBy(F.col("running_discount_pct_avg_3_prior_rounded_p95").desc())
)
