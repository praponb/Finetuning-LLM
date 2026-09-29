from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc(), "id")
w2 = Window.partitionBy("status", "order_id")
w_ordered = Window.partitionBy("carrier").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy("cost", "id")
w6 = Window.partitionBy("status").orderBy("shipped_date", "id")
w7 = Window.partitionBy("order_id", "carrier").orderBy("prev_cost", "id")
w8 = Window.partitionBy("order_id").orderBy("status_max_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

ranked = (
    df_shipments
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

with_windows = (
    ranked
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.max("cost").over(w2).alias("status_max_cost"),
        F.first("carrier", ignorenulls=True).over(w_ordered).alias("first_known_carrier"),
        F.avg("cost").over(w4).alias("cost_avg_6"),
        F.lag("cost", 1).over(w5).alias("prev_cost"),
        F.lag("cost", 1, 0).over(w6).alias("prev_cost_2"),
    )
)

latest = (
    with_windows
    .withColumn("dedup_rank", F.row_number().over(w7))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

scored = (
    latest
    .select(
        "id",
        "delivered_date",
        "cost",
        "cost_avg_6",
        "prev_cost",
        "prev_cost_2",
        F.sum("prev_cost_2").over(w8).alias("running_sum_prev_cost_2"),
        F.avg("prev_cost_2").over(w8).alias("running_avg_prev_cost_2"),
        F.min("prev_cost_2").over(w8).alias("running_min_prev_cost_2"),
    )
)

base = (
    scored
    .filter(F.col("cost_avg_6") == 61)
    .select(
        "id",
        "delivered_date",
        "cost_avg_6",
        "prev_cost",
        "prev_cost_2",
        "running_sum_prev_cost_2",
        "running_min_prev_cost_2",
        F.weekofyear("delivered_date").alias("delivered_date_weekofyear"),
    )
)

filtered = (
    base
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-03-17")))
    .select(
        "id",
        "delivered_date",
        "cost_avg_6",
        "prev_cost",
        "prev_cost_2",
        "running_sum_prev_cost_2",
        "running_min_prev_cost_2",
        "delivered_date_weekofyear",
        F.round("running_sum_prev_cost_2", 0).alias("running_sum_prev_cost_2_rounded"),
        F.col("running_sum_prev_cost_2").cast("int").alias("running_sum_prev_cost_2_int"),
    )
)

result = (
    filtered
    .withColumn("week_start", F.date_trunc("WEEK", "delivered_date"))
    .withColumn("activity_month", F.date_trunc("MONTH", "delivered_date"))
    .withColumn("quarter_start", F.date_trunc("QUARTER", "delivered_date"))
    .groupingSets(
        [["week_start", "activity_month", "quarter_start"], ["week_start", "activity_month"], []],
        "week_start",
        "activity_month",
        "quarter_start",
    )
    .agg(
        F.percentile_approx("delivered_date_weekofyear", 0.95).alias("delivered_date_weekofyear_p95"),
        F.countDistinct("id").alias("distinct_ids"),
        F.round(F.avg("running_sum_prev_cost_2_int"), 2).alias("avg_running_sum_prev_cost_2_int"),
        F.max("running_sum_prev_cost_2_int").alias("max_running_sum_prev_cost_2_int"),
        F.avg("delivered_date_weekofyear").alias("avg_delivered_date_weekofyear"),
        F.round(F.avg("prev_cost_2"), 2).alias("avg_prev_cost_2"),
        F.grouping("week_start").alias("grouping_level"),
    )
    .orderBy("avg_running_sum_prev_cost_2_int")
)
