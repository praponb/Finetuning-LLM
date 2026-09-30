from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("currency").orderBy(F.col("element").desc())
w2 = Window.orderBy(F.col("element_cume_dist").asc_nulls_last())
w3 = Window.partitionBy("plan").orderBy(F.col("element").desc_nulls_first())
w4 = Window.partitionBy("currency").orderBy(F.col("element_cume_dist").asc_nulls_first())
w5 = Window.partitionBy("plan").orderBy(F.col("element_cume_dist_percent_rank").asc_nulls_first())
w_hist = Window.orderBy(F.col("element_cume_dist_filled").desc())
w7 = Window.partitionBy("plan").orderBy(F.col("element_rank_2").desc_nulls_last())
w8 = Window.orderBy(F.col("element_cume_dist_percent_rank").asc_nulls_first())
w_running = Window.partitionBy("plan")

exploded = (
    df_price_tiers
    .select(
        "id",
        "plan",
        "effective_from",
        "currency",
        F.explode("discounts").alias("element"),
    )
)

enriched = exploded.withColumn("element_cume_dist", F.cume_dist().over(w))

metrics = (
    enriched
    .select(
        "*",
        F.percent_rank().over(w2).alias("element_cume_dist_percent_rank"),
        F.rank().over(w3).alias("element_rank"),
    )
)

filtered = (
    metrics
    .filter(F.col("currency").isin(["USD", "THB", "EUR"]))
    .select(
        "*",
        F.coalesce("element_cume_dist", F.lit(0)).alias("element_cume_dist_filled"),
        F.date_trunc("YEAR", "effective_from").alias("effective_from_period"),
    )
)

remaining_keys = (
    filtered
    .select("plan", "id")
    .intersect(
        df_price_tiers
        .filter(F.col("plan") == "basic")
        .select("plan", "id")
    )
)

kept_rows = (
    filtered
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.plan") == F.col("k.plan")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .filter(F.col("element_cume_dist") > 0.79)
    .select(
        "plan",
        "element",
        "element_cume_dist_percent_rank",
        "element_rank",
        "element_cume_dist_filled",
        F.rank().over(w4).alias("element_cume_dist_rank"),
        F.rank().over(w5).alias("element_cume_dist_percent_rank_rank"),
        F.percent_rank().over(w_hist).alias("element_cume_dist_filled_percent_rank"),
        F.rank().over(w3).alias("element_rank_2"),
    )
)

metrics_price_tiers = (
    scored
    .select(
        "*",
        F.cume_dist().over(w7).alias("element_rank_2_cume_dist"),
        F.percent_rank().over(w8).alias("element_cume_dist_percent_rank_percent_rank"),
        F.avg("element_cume_dist_filled").over(w_running).alias("plan_avg_element_cume_dist_filled"),
    )
)

result = (
    metrics_price_tiers
    .select(
        "plan",
        F.col("element_cume_dist_filled").alias("ELEMENT_CUME_DIST_FILLED"),
        F.col("element_cume_dist_percent_rank").alias("ELEMENT_CUME_DIST_PERCENT_RANK"),
        F.col("element_rank_2_cume_dist").alias("ELEMENT_RANK_2_CUME_DIST_VAL"),
    )
    .unpivot(
        ids=["plan"],
        values=["ELEMENT_CUME_DIST_FILLED", "ELEMENT_CUME_DIST_PERCENT_RANK", "ELEMENT_RANK_2_CUME_DIST_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
