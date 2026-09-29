from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("is_returned", "id")
w2 = Window.partitionBy("carrier").orderBy(F.col("delivered_date").asc_nulls_first(), "id")
w3 = Window.partitionBy("carrier").orderBy(F.col("is_returned").desc(), "id")
w4 = Window.partitionBy("carrier").orderBy("dedup_rank_str", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-5, Window.currentRow)
w6 = Window.partitionBy("row_num_plus_cost_tier").orderBy(F.col("dedup_rank_str_per_ups").desc())

base = (
    df_shipments
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-07-02"))) & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.when(F.col("status") == "RETURNED", 1).otherwise(0).alias("is_returned"),
    )
)

latest = (
    base
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

deduped = (
    latest
    .withColumn("row_num", F.row_number().over(w2))
    .filter(F.col("row_num") == 1)
)

valid = (
    deduped
    .filter(F.col("carrier").isNotNull() & (~F.col("carrier").eqNullSafe(F.lit("Flash"))))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "is_returned",
        "dedup_rank",
        "row_num",
        F.col("dedup_rank").cast("double").alias("dedup_rank_str"),
        (F.col("row_num") + F.col("cost")).alias("row_num_plus_cost"),
        F.coalesce("carrier", F.lit("N/A")).alias("resolved_carrier"),
    )
)

with_windows = (
    valid
    .withColumn("is_returned_bucket", F.ntile(5).over(w3))
    .withColumn("first_known_dedup_rank_str", F.first("dedup_rank_str", ignorenulls=True).over(w4))
    .withColumn("row_num_plus_cost_avg_5", F.avg("row_num_plus_cost").over(w5))
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("row_num_plus_cost") >= 79, "HIGH").otherwise("LOW").alias("row_num_plus_cost_tier"),
        F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.size(F.collect_set("carrier")).alias("num_carriers"),
        F.round(F.sum("dedup_rank_str") / F.nullif(F.count(F.when(F.col("carrier") == "UPS", 1)), F.lit(0)), 2).alias("dedup_rank_str_per_ups"),
    )
    .withColumn("dedup_rank_str_per_ups_dense_rank", F.dense_rank().over(w6))
    .orderBy("num_carriers")
)
