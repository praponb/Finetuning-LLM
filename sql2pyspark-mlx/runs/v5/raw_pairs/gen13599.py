from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w2 = Window.partitionBy("order_id").orderBy("shipped_date_plus_3d", "id")
w3 = Window.partitionBy("order_id").orderBy(F.col("row_seq_2").desc_nulls_last(), "id")
w4 = Window.partitionBy("order_id").orderBy("next_cost", "id").rowsBetween(-3, Window.currentRow)
w5 = Window.partitionBy("order_id").orderBy(F.col("row_seq").desc())
w6 = Window.orderBy(F.col("last4_count_prev_cost").desc(), "id")
w7 = Window.orderBy("last4_avg_prev_cost", "id")

base = (
    df_shipments
    .filter(F.col("cost").isNotNull())
    .select(
        "id",
        "order_id",
        "delivered_date",
        "cost",
        F.date_add("shipped_date", 3).alias("shipped_date_plus_3d"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

with_windows = (
    base
    .select(
        "*",
        F.lead("cost").over(w_hist).alias("next_cost"),
        F.lag("cost").over(w_hist).alias("prev_cost"),
        F.row_number().over(w_hist).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
    )
)

ranked = (
    with_windows
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

scored = (
    ranked
    .filter(F.col("cost") >= 36)
    .select(
        "id",
        "row_seq",
        F.avg("prev_cost").over(w4).alias("last4_avg_prev_cost"),
        F.count("prev_cost").over(w4).alias("last4_count_prev_cost"),
        F.min("prev_cost").over(w4).alias("last4_min_prev_cost"),
        F.rank().over(w5).alias("row_seq_rank"),
    )
)

with_windows_shipments = (
    scored
    .select(
        "id",
        "row_seq",
        "last4_count_prev_cost",
        "last4_min_prev_cost",
        F.row_number().over(w6).alias("row_seq_2"),
        F.lag("last4_min_prev_cost", 1).over(w7).alias("prev_last4_min_prev_cost"),
    )
)

result = (
    with_windows_shipments
    .groupBy(
        F.when(F.col("last4_min_prev_cost") >= 87, "LARGE").otherwise("SMALL").alias("last4_min_prev_cost_tier"),
    )
    .agg(
        F.max("row_seq").alias("max_row_seq"),
        F.percentile_approx("row_seq_2", 0.9).alias("row_seq_2_p90"),
        F.round(F.avg("row_seq_2"), 2).alias("avg_row_seq_2"),
    )
)
