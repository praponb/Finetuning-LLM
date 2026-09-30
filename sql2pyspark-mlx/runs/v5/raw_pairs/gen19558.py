from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "carrier").orderBy(F.col("shipped_date_period").cast("bigint")).rangeBetween(-2592000, Window.currentRow)
w2 = Window.partitionBy("order_id").orderBy(F.col("shipped_date").desc(), "id")
w_part = Window.partitionBy("order_id").orderBy(F.col("cost").asc_nulls_last())
w4 = Window.orderBy(F.col("cost").asc_nulls_last())
w_grp = Window.partitionBy("status").orderBy("row_seq", "id")
w_grp_2 = Window.partitionBy("status").orderBy("cost", "id")

base = (
    df_shipments
    .filter(F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.concat_ws("-", "carrier", "status").alias("carrier_status_key"),
        F.date_trunc("MONTH", "shipped_date").alias("shipped_date_period"),
    )
)

enriched = (
    base
    .select(
        "id",
        "delivered_date",
        "cost",
        "status",
        F.avg("cost").over(w).alias("cost_avg_30d"),
        F.row_number().over(w2).alias("row_seq"),
        F.rank().over(w_part).alias("cost_rank"),
        F.cume_dist().over(w4).alias("cost_cume_dist"),
    )
)

enriched_shipments = (
    enriched
    .select(
        "*",
        F.lead("cost").over(w_grp).alias("next_cost"),
        F.lag("row_seq").over(w_grp).alias("prev_row_seq"),
        F.row_number().over(w_grp).alias("row_seq_2"),
        F.row_number().over(w_grp_2).alias("row_seq_3"),
    )
)

latest = (
    enriched_shipments
    .withColumn("row_num", F.row_number().over(w_grp))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .withColumn("cost_rank_tier", F.when(F.col("cost_rank") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("row_seq_2_tier", F.when(F.col("row_seq_2") >= 8, "HIGH").otherwise("LOW"))
    .withColumn("row_seq_3_tier", F.when(F.col("row_seq_3") >= 9, "LARGE").otherwise("SMALL"))
    .groupingSets(
        [["cost_rank_tier", "row_seq_2_tier", "row_seq_3_tier"], ["cost_rank_tier", "row_seq_2_tier"]],
        "cost_rank_tier",
        "row_seq_2_tier",
        "row_seq_3_tier",
    )
    .agg(
        F.count_if(F.col("status") == "RETURNED").alias("returned_events"),
        F.round(F.sum("cost_avg_30d") / F.nullif(F.count(F.when(F.col("status") == "IN_TRANSIT", 1)), F.lit(0)), 2).alias("cost_avg_30d_per_in_transit"),
        F.sum(F.when(F.col("status") == "IN_TRANSIT", F.col("row_seq_3")).otherwise(0)).alias("in_transit_row_seq_3"),
        F.grouping_id().alias("gid"),
    )
    .filter(F.col("returned_events") >= 1)
)
