from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "status").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.orderBy(F.col("last3_count_cost").desc())
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("carrier").orderBy(F.col("last3_sum_cost").desc_nulls_first())
w5 = Window.orderBy(F.col("last3_sum_cost").desc(), "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("last3_max_cost").desc(), "id")
w7 = Window.partitionBy("carrier").orderBy("last3_count_cost_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("carrier").orderBy(F.col("last3_sum_cost_dense_rank").desc_nulls_last(), "id")

scored = (
    df_shipments
    .filter(F.col("delivered_date") > "2023-03-01")
    .select(
        "id",
        "carrier",
        "shipped_date",
        F.min("cost").over(w).alias("last3_min_cost"),
        F.sum("cost").over(w).alias("last3_sum_cost"),
        F.max("cost").over(w).alias("last3_max_cost"),
        F.count("cost").over(w).alias("last3_count_cost"),
    )
)

scored_shipments = (
    scored
    .select(
        "id",
        "carrier",
        "shipped_date",
        "last3_sum_cost",
        "last3_max_cost",
        F.dense_rank().over(w2).alias("last3_count_cost_dense_rank"),
        F.round((F.col("last3_min_cost") / F.sum("last3_min_cost").over(w3)), 4).alias("last3_min_cost_share"),
        F.dense_rank().over(w4).alias("last3_sum_cost_dense_rank"),
    )
)

enriched = (
    scored_shipments
    .select(
        "*",
        F.ntile(5).over(w5).alias("last3_sum_cost_bucket"),
        F.row_number().over(w6).alias("event_seq"),
        F.sum("last3_min_cost_share").over(w7).alias("cumulative_last3_min_cost_share"),
    )
)

deduped = (
    enriched
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "last3_sum_cost",
        "last3_max_cost",
        "last3_min_cost_share",
        "last3_sum_cost_dense_rank",
        "event_seq",
    )
)

summary = (
    deduped
    .groupBy(F.date_trunc("WEEK", "shipped_date").alias("week_start"))
    .agg(
        F.max("last3_sum_cost").alias("max_last3_sum_cost"),
        F.sum(F.when(F.col("carrier") == "Kerry", F.col("last3_sum_cost_dense_rank"))).alias("kerry_total_last3_sum_cost_dense_rank"),
        F.round(F.avg("last3_sum_cost_dense_rank"), 2).alias("avg_last3_sum_cost_dense_rank"),
        F.round(F.sum("event_seq") / F.nullif(F.count(F.when(F.col("carrier") == "DHL", 1)), F.lit(0)), 2).alias("event_seq_per_dhl"),
        F.percentile_approx("last3_sum_cost", 0.9).alias("last3_sum_cost_p90"),
    )
    .filter(F.col("last3_sum_cost_p90") > 0)
)

result = (
    summary
    .select(
        "week_start",
        "max_last3_sum_cost",
        "kerry_total_last3_sum_cost_dense_rank",
        "avg_last3_sum_cost_dense_rank",
        "last3_sum_cost_p90",
    )
)
