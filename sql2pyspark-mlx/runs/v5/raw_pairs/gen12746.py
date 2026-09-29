from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id")
w2 = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("carrier", "status").orderBy("cost", "id")
w4 = Window.partitionBy("order_id").orderBy(F.col("cost").desc(), "id")
w5 = Window.partitionBy("status").orderBy(F.col("row_seq").desc_nulls_first())
w6 = Window.partitionBy("status").orderBy(F.col("cost_bucket").desc(), "id")
w7 = Window.partitionBy("status").orderBy(F.col("latest_rank").desc(), "id")

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.lit("2023-07-15").cast("date")) & (F.year("delivered_date") >= 2023))
    .withColumn("status_carrier", F.concat("status", F.lit(" - "), "carrier"))
)

scored = (
    filtered
    .select(
        "id",
        "status",
        "status_carrier",
        F.min("cost").over(w).alias("order_id_min_cost"),
        F.avg("cost").over(w2).alias("cost_avg_2"),
        F.row_number().over(w3).alias("row_seq"),
        F.ntile(4).over(w4).alias("cost_bucket"),
    )
)

latest = (
    scored
    .withColumn("rank_in_group", F.rank().over(w5))
    .filter(F.col("rank_in_group") == 1)
)

latest_shipments = (
    latest
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

deduped = (
    latest_shipments
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
)

stats = (
    deduped
    .groupBy("status")
    .agg(
        F.sort_array(F.collect_set("status")).alias("status_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("order_id_min_cost", 0.95).alias("order_id_min_cost_p95"),
        F.round(F.sum("cost_avg_2") / F.nullif(F.count(F.when(F.col("status") == "LOST", 1)), F.lit(0)), 2).alias("cost_avg_2_per_lost"),
    )
)

result = (
    stats
    .select(
        "status",
        "status_list",
        "distinct_ids",
        "order_id_min_cost_p95",
        F.when(F.col("status").rlike("^(RETURNED|LOST)$"), "GROUP_A").otherwise("PRIORITY").alias("status_zone"),
    )
)
