from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("credit_limit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("credit_limit", "id")
w3 = Window.partitionBy("city").orderBy(F.col("row_seq").desc(), "id")
w4 = Window.orderBy("first_known_credit_limit", "id")
w5 = Window.orderBy(F.col("prev_first_known_credit_limit").desc(), "id")
w6 = Window.orderBy("credit_limit", "id").rowsBetween(-3, Window.currentRow)
w7 = Window.orderBy(F.col("credit_limit").asc_nulls_last())
w8 = Window.orderBy(F.col("prev_first_known_credit_limit").desc_nulls_first())
w9 = Window.orderBy(F.col("prev_first_known_credit_limit_rounded").desc_nulls_last())
w10 = Window.orderBy("next_credit_limit_avg_3")

scored = (
    df_customers
    .filter(F.lower("name").like("%ob %"))
    .select(
        "*",
        F.first("credit_limit", ignorenulls=True).over(w_running).alias("first_known_credit_limit"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select("id", "credit_limit", "first_known_credit_limit")
)

enriched = (
    ranked
    .select(
        "*",
        F.lag("first_known_credit_limit").over(w4).alias("prev_first_known_credit_limit"),
        F.lead("first_known_credit_limit").over(w4).alias("next_first_known_credit_limit"),
        F.lead("credit_limit").over(w4).alias("next_credit_limit"),
        F.row_number().over(w4).alias("event_seq"),
    )
)

with_windows = (
    enriched
    .select(
        "id",
        "credit_limit",
        "first_known_credit_limit",
        "prev_first_known_credit_limit",
        "next_first_known_credit_limit",
        F.row_number().over(w5).alias("event_seq_2"),
        F.avg("next_credit_limit").over(w6).alias("next_credit_limit_avg_3"),
    )
)

filtered = (
    with_windows
    .filter(F.col("first_known_credit_limit").between(8600, 12300))
    .select(
        "id",
        "credit_limit",
        "first_known_credit_limit",
        "prev_first_known_credit_limit",
        "event_seq_2",
        "next_credit_limit_avg_3",
        F.round("prev_first_known_credit_limit", 1).alias("prev_first_known_credit_limit_rounded"),
        F.ceil("next_credit_limit_avg_3").alias("next_credit_limit_avg_3_whole"),
    )
)

kept_keys = (
    filtered
    .select("id")
    .subtract(
        df_customers
        .filter(F.col("segment") == "SMB")
        .select("id")
    )
)

reconciled = (
    filtered
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored_customers = (
    reconciled
    .select(
        "id",
        "credit_limit",
        "prev_first_known_credit_limit",
        "event_seq_2",
        "next_credit_limit_avg_3_whole",
        F.cume_dist().over(w7).alias("credit_limit_cume_dist"),
        F.percent_rank().over(w8).alias("prev_first_known_credit_limit_percent_rank"),
        F.dense_rank().over(w9).alias("prev_first_known_credit_limit_rounded_dense_rank"),
        F.cume_dist().over(w10).alias("next_credit_limit_avg_3_cume_dist"),
    )
)

totals = (
    scored_customers
    .withColumn("prev_first_known_credit_limit_rounded_dense_rank_tier", F.when(F.col("prev_first_known_credit_limit_rounded_dense_rank") >= 8, "LARGE").otherwise("SMALL"))
    .withColumn("event_seq_2_tier", F.when(F.col("event_seq_2") >= 7, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["prev_first_known_credit_limit_rounded_dense_rank_tier", "event_seq_2_tier"], ["prev_first_known_credit_limit_rounded_dense_rank_tier"]],
        "prev_first_known_credit_limit_rounded_dense_rank_tier",
        "event_seq_2_tier",
    )
    .agg(
        F.avg("next_credit_limit_avg_3_cume_dist").alias("avg_next_credit_limit_avg_3_cume_dist"),
        F.percentile_approx("prev_first_known_credit_limit_percent_rank", [0.25, 0.5, 0.75]).alias("prev_first_known_credit_limit_percent_rank_quartiles"),
    )
)

result = (
    totals
    .select(
        "prev_first_known_credit_limit_rounded_dense_rank_tier",
        "event_seq_2_tier",
        "avg_next_credit_limit_avg_3_cume_dist",
        "prev_first_known_credit_limit_percent_rank_quartiles",
    )
)
