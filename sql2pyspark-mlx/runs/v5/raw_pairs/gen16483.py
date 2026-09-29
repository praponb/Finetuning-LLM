from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("shipped_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("carrier").orderBy("delivered_date", "id")
w3 = Window.orderBy("cost", "id").rowsBetween(-2, -1)
w4 = Window.orderBy(F.col("cost").asc_nulls_first())
w5 = Window.partitionBy("carrier").orderBy(F.col("cost_avg_2_prior").asc_nulls_last(), "id")
w6 = Window.orderBy(F.col("cost").desc(), "id")
w7 = Window.orderBy("seq_no", "id")
w_recent = Window.orderBy("delivered_date", "id")
w9 = Window.orderBy("next_seq_no", "id")
w10 = Window.orderBy("seq_no", "id").rowsBetween(-2, Window.currentRow)
w_hist = Window.orderBy(F.col("seq_no").desc_nulls_last())
w12 = Window.orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w_grp = Window.orderBy("delivered_date_quarter")

deduped = (
    df_shipments
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .select("id", "carrier", "delivered_date", "cost")
)

scored = (
    deduped
    .filter(F.col("cost") >= 42)
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.avg("cost").over(w3).alias("cost_avg_2_prior"),
        F.dense_rank().over(w4).alias("cost_dense_rank"),
    )
)

deduped_shipments = (
    scored
    .withColumn("rank_in_group", F.row_number().over(w5))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

base = (
    deduped_shipments
    .filter(~F.col("carrier").eqNullSafe(F.lit("UPS")))
    .select(
        "id",
        "delivered_date",
        "cost",
        "seq_no",
        F.quarter("delivered_date").alias("delivered_date_quarter"),
    )
)

with_windows = (
    base
    .select(
        "id",
        "cost",
        "seq_no",
        "delivered_date_quarter",
        F.ntile(3).over(w6).alias("cost_bucket"),
        F.lead("seq_no", 1).over(w7).alias("next_seq_no"),
        F.row_number().over(w_recent).alias("seq_no_2"),
    )
)

base_shipments = (
    with_windows
    .filter(F.col("cost_bucket") >= 5)
    .select(
        "id",
        "cost",
        "seq_no",
        "delivered_date_quarter",
        "next_seq_no",
        "seq_no_2",
        F.coalesce("delivered_date_quarter", F.lit(0)).alias("delivered_date_quarter_filled"),
    )
)

scored_shipments = (
    base_shipments
    .select(
        "id",
        "cost",
        "delivered_date_quarter",
        "next_seq_no",
        "seq_no_2",
        "delivered_date_quarter_filled",
        F.row_number().over(w9).alias("row_seq"),
        F.avg("delivered_date_quarter_filled").over(w10).alias("delivered_date_quarter_filled_avg_2"),
        F.rank().over(w_hist).alias("seq_no_rank"),
        F.avg("cost").over(w12).alias("cost_avg_6"),
        F.rank().over(w_grp).alias("delivered_date_quarter_rank"),
    )
)

result = (
    scored_shipments
    .groupBy(
        F.when(F.col("cost") >= 90, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.min("next_seq_no").alias("min_next_seq_no"),
        F.round(F.avg("delivered_date_quarter_filled"), 2).alias("avg_delivered_date_quarter_filled"),
        F.avg("delivered_date_quarter_rank").alias("avg_delivered_date_quarter_rank"),
    )
)
