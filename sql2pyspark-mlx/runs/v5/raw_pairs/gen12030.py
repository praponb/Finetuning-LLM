from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier").orderBy("shipped_date_year", "id")
w2 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy(F.col("row_seq").desc_nulls_last(), "id")
w4 = Window.partitionBy("carrier").orderBy("shipped_date", "id").rowsBetween(-6, -1)
w_recent = Window.orderBy(F.col("cost").desc(), "id")
w6 = Window.partitionBy("carrier").orderBy(F.col("prev_cost").asc_nulls_last())
w_grp = Window.partitionBy("carrier").orderBy(F.col("next_shipped_date_year").desc_nulls_first())
w8 = Window.orderBy("next_shipped_date_year_cume_dist", "id")
w9 = Window.orderBy(F.col("next_cost_avg_6_prior").desc_nulls_last())
w10 = Window.orderBy("shipped_date", "id")
w11 = Window.partitionBy("carrier").orderBy("prev_cost", "id")
w12 = Window.orderBy("row_seq_2", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("shipped_date") >= F.to_date(F.lit("2023-09-16")))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.year("shipped_date").alias("shipped_date_year"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        F.lag("cost").over(w).alias("prev_cost"),
        F.lead("cost").over(w).alias("next_cost"),
        F.lead("shipped_date_year").over(w).alias("next_shipped_date_year"),
        F.row_number().over(w).alias("row_seq"),
        F.sum("shipped_date_year").over(w2).alias("cumulative_shipped_date_year"),
    )
)

ranked = (
    with_windows
    .withColumn("latest_rank", F.row_number().over(w3))
    .filter(F.col("latest_rank") == 1)
)

enriched = (
    ranked
    .filter(F.col("next_shipped_date_year") >= 31)
    .select(
        "*",
        F.avg("next_cost").over(w4).alias("next_cost_avg_6_prior"),
        F.row_number().over(w_recent).alias("row_seq_2"),
        F.dense_rank().over(w6).alias("prev_cost_dense_rank"),
        F.cume_dist().over(w_grp).alias("next_shipped_date_year_cume_dist"),
    )
)

valid_shipments = (
    enriched
    .filter(F.col("shipped_date") > F.lit("2023-05-23").cast("date"))
    .select(
        "id",
        "carrier",
        "shipped_date",
        "cost",
        "prev_cost",
        "next_cost",
        "next_shipped_date_year",
        "row_seq",
        "latest_rank",
        "next_cost_avg_6_prior",
        "row_seq_2",
        "next_shipped_date_year_cume_dist",
        F.coalesce("row_seq_2", F.lit(0)).alias("row_seq_2_filled"),
        F.round((F.col("row_seq") * 0.9), 2).alias("adjusted_row_seq"),
        F.coalesce("carrier", F.lit("UNKNOWN")).alias("resolved_carrier"),
    )
)

enriched_shipments = (
    valid_shipments
    .select(
        "*",
        F.ntile(3).over(w8).alias("next_shipped_date_year_cume_dist_bucket"),
        F.cume_dist().over(w9).alias("next_cost_avg_6_prior_cume_dist"),
        F.lead("next_cost", 1, 0).over(w10).alias("next_next_cost"),
    )
)

valid_shipments_2 = (
    enriched_shipments
    .filter((F.col("shipped_date") >= F.lit("2023-07-08").cast("date")) & (F.col("row_seq_2_filled") <= 48))
    .select(
        "*",
        F.round("cost", 1).alias("cost_rounded"),
        F.coalesce("carrier", F.lit("OTHER")).alias("carrier_clean"),
    )
)

result = (
    valid_shipments_2
    .select(
        "id",
        "shipped_date",
        "cost",
        "next_shipped_date_year",
        "row_seq",
        "next_cost_avg_6_prior",
        "next_shipped_date_year_cume_dist",
        "resolved_carrier",
        "next_shipped_date_year_cume_dist_bucket",
        "cost_rounded",
        F.row_number().over(w11).alias("row_seq_3"),
        F.sum("next_next_cost").over(w12).alias("running_next_next_cost"),
    )
)
