from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("shipped_date", "id")
w3 = Window.partitionBy("status").orderBy(F.col("delivered_date").desc(), "id")
w4 = Window.partitionBy("carrier")
w5 = Window.partitionBy("status").orderBy("cost", "id")
w6 = Window.orderBy(F.col("next_cost_whole").asc_nulls_last())
w7 = Window.orderBy("row_seq", "id")
w8 = Window.partitionBy("carrier").orderBy(F.col("carrier_avg_cost_whole").desc(), "id")
w9 = Window.partitionBy("status", "carrier").orderBy(F.col("cost").asc_nulls_first())

filtered = (
    df_shipments
    .filter((F.col("shipped_date") > F.to_date(F.lit("2023-08-15"))) & (F.col("cost") >= 42))
    .withColumn("cost_whole", F.floor("cost"))
    .withColumn("carrier_filled", F.coalesce("carrier", F.lit("DIRECT")))
)

with_windows = (
    filtered
    .filter(F.col("cost_whole") != 29)
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        "carrier_filled",
        F.last("cost", ignorenulls=True).over(w).alias("last_known_cost"),
        F.lag("cost", 2).over(w2).alias("prev_cost"),
        F.row_number().over(w3).alias("row_seq"),
        F.avg("cost_whole").over(w4).alias("carrier_avg_cost_whole"),
        F.lead("cost_whole", 1).over(w5).alias("next_cost_whole"),
    )
)

with_windows_shipments = (
    with_windows
    .withColumn("next_cost_whole_cume_dist", F.cume_dist().over(w6))
    .withColumn("row_seq_bucket", F.ntile(5).over(w7))
    .withColumn("row_seq_2", F.row_number().over(w8))
)

valid = (
    with_windows_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-08-24").cast("date")) & F.col("carrier").isNotNull())
    .select(
        "*",
        (F.col("last_known_cost") / F.nullif("next_cost_whole_cume_dist", F.lit(0))).alias("last_known_cost_per_next_cost_whole_cume_dist"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_clean"),
    )
)

ranked = (
    valid
    .withColumn("latest_rank", F.rank().over(w9))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("prev_cost") <= 96)
    .withColumn("carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist", (F.col("carrier_avg_cost_whole") * F.col("last_known_cost_per_next_cost_whole_cume_dist")))
    .withColumn("resolved_carrier", F.coalesce("carrier", F.lit("UNKNOWN")))
)

result = (
    base
    .select(
        "id",
        "status",
        "prev_cost",
        "last_known_cost_per_next_cost_whole_cume_dist",
        "carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist",
    )
    .unpivot(
        ids=["id", "status"],
        values=["prev_cost", "last_known_cost_per_next_cost_whole_cume_dist", "carrier_avg_cost_whole_times_last_known_cost_per_next_cost_whole_cume_dist"],
        variableColumnName="metric",
        valueColumnName="val",
    )
)
