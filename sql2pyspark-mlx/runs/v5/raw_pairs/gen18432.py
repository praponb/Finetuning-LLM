from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("carrier").orderBy("cost", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("cost").asc_nulls_first())
w3 = Window.partitionBy("carrier", "order_id").orderBy(F.col("last3_min_cost").desc_nulls_last(), "id")
w_grp = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(-3, Window.currentRow)

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.lit("2023-06-09").cast("date")) & F.col("carrier").isNotNull())
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("N/A")))
)

scored = (
    base
    .select(
        "*",
        F.max("cost").over(w_part).alias("last3_max_cost"),
        F.min("cost").over(w_part).alias("last3_min_cost"),
        F.sum("cost").over(w_part).alias("last3_sum_cost"),
        F.rank().over(w2).alias("cost_rank"),
    )
)

deduped = (
    scored
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "carrier_filled",
        "last3_max_cost",
    )
)

enriched = (
    deduped
    .filter(F.col("order_id").isin([18, 36, 40]))
    .select(
        "id",
        "order_id",
        "shipped_date",
        "cost",
        "carrier_filled",
        F.avg("cost").over(w_grp).alias("last4_avg_cost"),
        F.sum("cost").over(w_grp).alias("last4_sum_cost"),
        F.count("cost").over(w_grp).alias("last4_count_cost"),
        F.max("cost").over(w_grp).alias("last4_max_cost"),
    )
)

result = (
    enriched
    .groupBy("order_id")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.avg("last4_count_cost").alias("avg_last4_count_cost"),
        F.count("*").alias("cnt"),
        F.percentile_approx("last4_max_cost", 0.9).alias("last4_max_cost_p90"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .orderBy("order_id")
)
