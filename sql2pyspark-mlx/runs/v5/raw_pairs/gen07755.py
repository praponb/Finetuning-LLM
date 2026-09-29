from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status").orderBy(F.col("shipped_date").desc(), "id")
w2 = Window.partitionBy("status").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier")
w4 = Window.partitionBy("status", "carrier")
w5 = Window.partitionBy("status").orderBy("status_avg_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.partitionBy("status").orderBy("cumulative_cost", "id")
w8 = Window.partitionBy("order_id").orderBy(F.col("cumulative_cost").desc(), "id")
w9 = Window.partitionBy("order_id", "carrier").orderBy(F.col("first_known_carrier_stddev_samp_cost").desc_nulls_first(), "id")

ranked = (
    df_shipments
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    ranked
    .filter((F.col("delivered_date") > F.lit("2023-05-04").cast("date")) & (~F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.lower("status").alias("status_lower"),
    )
)

metrics = (
    filtered
    .filter(F.col("shipped_date") < "2024-07-01")
    .select(
        "*",
        F.sum("cost").over(w2).alias("cumulative_cost"),
        F.stddev_samp("cost").over(w3).alias("carrier_stddev_samp_cost"),
        F.avg("cost").over(w4).alias("status_avg_cost"),
    )
)

enriched = (
    metrics
    .select(
        "*",
        F.sum("cumulative_cost").over(w5).alias("cumulative_cumulative_cost"),
        F.first("carrier_stddev_samp_cost", ignorenulls=True).over(w6).alias("first_known_carrier_stddev_samp_cost"),
        F.lag("cumulative_cost", 1).over(w7).alias("prev_cumulative_cost"),
        F.min("cost").over(w3).alias("carrier_min_cost"),
    )
)

latest = (
    enriched
    .withColumn("row_num", F.row_number().over(w8))
    .filter(F.col("row_num") == 1)
)

deduped = (
    latest
    .withColumn("rank_in_group", F.row_number().over(w9))
    .filter(F.col("rank_in_group") == 1)
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        "cumulative_cost",
        "carrier_stddev_samp_cost",
        "status_avg_cost",
        "cumulative_cumulative_cost",
        "first_known_carrier_stddev_samp_cost",
        "carrier_min_cost",
        "row_num",
    )
)

cleaned = (
    deduped
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-03-28")))
    .select(
        "id",
        "delivered_date",
        "status",
        "carrier_stddev_samp_cost",
        "status_avg_cost",
        "cumulative_cumulative_cost",
        "first_known_carrier_stddev_samp_cost",
        "carrier_min_cost",
        "row_num",
        F.add_months("delivered_date", 12).alias("delivered_date_plus_months"),
    )
)

result = (
    cleaned
    .select(
        "id",
        "delivered_date",
        "status",
        "carrier_stddev_samp_cost",
        "status_avg_cost",
        "cumulative_cumulative_cost",
        "delivered_date_plus_months",
        F.when(F.col("status").rlike("^(DELIVERED|RETURNED)$"), "PRIORITY").otherwise("GROUP_B").alias("status_zone"),
    )
    .orderBy(F.col("cumulative_cumulative_cost").asc_nulls_first())
)
