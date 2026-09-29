from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier")
w2 = Window.partitionBy("status")
w3 = Window.orderBy("days_since_delivered_date", "id")
w4 = Window.partitionBy("carrier").orderBy("days_since_delivered_date", "id").rowsBetween(-6, -1)
w5 = Window.partitionBy("carrier", "status").orderBy(F.col("days_since_delivered_date").desc_nulls_last())
w6 = Window.orderBy(F.col("days_since_delivered_date_avg_6_prior").desc(), "id")
w7 = Window.orderBy("days_since_delivered_date", "id").rowsBetween(-3, Window.currentRow)
w8 = Window.orderBy("delivered_date", "id")
w_recent = Window.orderBy("days_since_delivered_date_rank", "id")
w10 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w11 = Window.orderBy(F.col("carrier_stddev_samp_days_shipped_date_to_delivered_date").desc())
w_grp = Window.orderBy("days_since_delivered_date_avg_6_prior_bucket", "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNull())
    .select(
        "id",
        "carrier",
        "delivered_date",
        "cost",
        "status",
        F.datediff(F.current_date(), "delivered_date").alias("days_since_delivered_date"),
        F.datediff("delivered_date", "shipped_date").alias("days_shipped_date_to_delivered_date"),
    )
)

metrics = (
    base
    .select(
        "id",
        "delivered_date",
        "cost",
        "days_since_delivered_date",
        F.stddev_samp("days_shipped_date_to_delivered_date").over(w).alias("carrier_stddev_samp_days_shipped_date_to_delivered_date"),
        F.round((F.col("cost") / F.sum("cost").over(w2)), 4).alias("cost_share"),
        F.lead("days_since_delivered_date", 2, 0).over(w3).alias("next_days_since_delivered_date"),
        F.avg("days_since_delivered_date").over(w4).alias("days_since_delivered_date_avg_6_prior"),
        F.rank().over(w5).alias("days_since_delivered_date_rank"),
    )
)

scored = (
    metrics
    .withColumn("days_since_delivered_date_avg_6_prior_bucket", F.ntile(10).over(w6))
    .withColumn("days_since_delivered_date_avg_3", F.avg("days_since_delivered_date").over(w7))
    .withColumn("row_seq", F.row_number().over(w8))
    .withColumn("prev_cost_share", F.lag("cost_share", 1).over(w8))
)

with_windows = (
    scored
    .select(
        "id",
        "delivered_date",
        "next_days_since_delivered_date",
        "days_since_delivered_date_avg_6_prior",
        "days_since_delivered_date_avg_6_prior_bucket",
        "days_since_delivered_date_avg_3",
        "row_seq",
        "prev_cost_share",
        F.lag("carrier_stddev_samp_days_shipped_date_to_delivered_date", 1).over(w_recent).alias("prev_carrier_stddev_samp_days_shipped_date_to_delivered_date"),
        F.first("cost", ignorenulls=True).over(w10).alias("first_known_cost"),
        F.rank().over(w11).alias("carrier_stddev_samp_days_shipped_date_to_delivered_date_rank"),
    )
)

metrics_shipments = (
    with_windows
    .select(
        "*",
        F.lag("carrier_stddev_samp_days_shipped_date_to_delivered_date_rank").over(w_grp).alias("prev_carrier_stddev_samp_days_shipped_date_to_delivered_date_rank"),
        F.lag("prev_carrier_stddev_samp_days_shipped_date_to_delivered_date").over(w_grp).alias("prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date"),
        F.lead("prev_carrier_stddev_samp_days_shipped_date_to_delivered_date").over(w_grp).alias("next_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date"),
        F.row_number().over(w_grp).alias("seq_no"),
    )
)

result = (
    metrics_shipments
    .groupBy(F.date_trunc("QUARTER", "delivered_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("row_count"),
        F.percentile_approx("days_since_delivered_date_avg_6_prior", [0.25, 0.5, 0.75]).alias("days_since_delivered_date_avg_6_prior_quartiles"),
        F.round(F.avg("prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date"), 2).alias("avg_prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date"),
    )
    .filter(F.col("avg_prev_prev_carrier_stddev_samp_days_shipped_date_to_delivered_date") < 1000000)
)
