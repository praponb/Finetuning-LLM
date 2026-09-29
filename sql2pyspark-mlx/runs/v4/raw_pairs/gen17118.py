from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("order_id")
w_recent = Window.partitionBy("order_id", "carrier").orderBy(F.col("cost").asc_nulls_last())
w4 = Window.orderBy(F.col("order_id_count_cost").desc_nulls_first())
w5 = Window.partitionBy("carrier").orderBy(F.col("cost_percent_rank").desc())

matched_keys = (
    df_shipments
    .select("carrier", "order_id")
    .subtract(
        df_shipments
        .filter(F.col("order_id").isin([23, 26, 33]))
        .select("carrier", "order_id")
    )
)

kept_rows = (
    df_shipments
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.carrier") == F.col("k.carrier")) & (F.col("s.order_id") == F.col("k.order_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .withColumn("cost_cume_dist", F.cume_dist().over(w))
    .withColumn("order_id_count_cost", F.count("cost").over(w2))
    .withColumn("cost_percent_rank", F.percent_rank().over(w_recent))
)

scored_shipments = (
    scored
    .select(
        "*",
        F.rank().over(w4).alias("order_id_count_cost_rank"),
        F.dense_rank().over(w5).alias("cost_percent_rank_dense_rank"),
    )
)

base = (
    scored_shipments
    .filter(F.col("carrier").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "cost",
        "status",
        "order_id_count_cost",
        "cost_percent_rank",
        "order_id_count_cost_rank",
        (F.col("cost") * F.col("cost_percent_rank_dense_rank")).alias("cost_times_cost_percent_rank_dense_rank"),
    )
)

result = (
    base
    .withColumn("cost_percent_rank_tier", F.when(F.col("cost_percent_rank") >= 0.85, "HIGH").otherwise("LOW"))
    .withColumn("cost_times_cost_percent_rank_dense_rank_tier", F.when(F.col("cost_times_cost_percent_rank_dense_rank") >= 64, "PREMIUM").otherwise("STANDARD"))
    .groupingSets(
        [["cost_percent_rank_tier", "cost_times_cost_percent_rank_dense_rank_tier"], ["cost_percent_rank_tier"], []],
        "cost_percent_rank_tier",
        "cost_times_cost_percent_rank_dense_rank_tier",
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.sum("cost").alias("total_cost"),
        F.min("cost_times_cost_percent_rank_dense_rank").alias("min_cost_times_cost_percent_rank_dense_rank"),
        F.round(F.avg("cost_times_cost_percent_rank_dense_rank"), 2).alias("avg_cost_times_cost_percent_rank_dense_rank"),
        F.count(F.when(F.col("status") == "IN_TRANSIT", 1)).alias("in_transit_count"),
        F.grouping_id().alias("grouping_level"),
    )
    .filter(F.col("avg_cost") < 0)
)
