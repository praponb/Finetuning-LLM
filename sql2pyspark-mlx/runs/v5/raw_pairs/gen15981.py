from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("status").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("status").orderBy(F.col("running_sum_cost_whole_filled").desc_nulls_last(), "id")

valid = (
    df_shipments
    .filter((F.col("delivered_date") > F.lit("2023-09-13").cast("date")) & F.col("order_id").isNotNull())
    .select(
        "id",
        "shipped_date",
        "cost",
        "status",
        F.floor("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("NONE")).alias("resolved_carrier"),
    )
)

cleaned = (
    valid
    .filter((F.col("shipped_date") > F.lit("2023-07-29").cast("date")) & F.col("cost_whole").between(27, 56))
    .select(
        "id",
        "shipped_date",
        "status",
        "cost_whole",
        "resolved_carrier",
        F.coalesce("cost_whole", F.lit(0)).alias("cost_whole_filled"),
    )
)

scored = (
    cleaned
    .select(
        "*",
        F.count("cost_whole_filled").over(w_running).alias("running_count_cost_whole_filled"),
        F.max("cost_whole_filled").over(w_running).alias("running_max_cost_whole_filled"),
        F.sum("cost_whole_filled").over(w_running).alias("running_sum_cost_whole_filled"),
    )
)

base = (
    scored
    .filter(F.col("shipped_date") > F.lit("2023-06-16").cast("date"))
    .withColumn("cost_whole_filled_filled", F.coalesce("cost_whole_filled", F.lit(0)))
)

ranked = (
    base
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .select(
        "id",
        "shipped_date",
        "cost_whole",
        "resolved_carrier",
        "cost_whole_filled",
        "running_count_cost_whole_filled",
        "running_max_cost_whole_filled",
        "running_sum_cost_whole_filled",
        "cost_whole_filled_filled",
    )
    .orderBy(F.col("cost_whole_filled").desc(), "cost_whole")
)
