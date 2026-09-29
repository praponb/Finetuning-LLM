from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("delivered_date", "id")
w_hist = Window.orderBy("seq_no", "id")
w3 = Window.orderBy("seq_no", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_shipments
    .filter(F.col("carrier").eqNullSafe(F.lit("Flash")))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.add_months("shipped_date", 1).alias("shipped_date_plus_months"),
    )
)

scored = (
    valid
    .select(
        "id",
        "shipped_date",
        "delivered_date",
        "shipped_date_plus_months",
        F.lag("cost").over(w).alias("prev_cost"),
        F.row_number().over(w).alias("seq_no"),
    )
)

scored_shipments = (
    scored
    .select(
        "id",
        "delivered_date",
        "shipped_date_plus_months",
        "prev_cost",
        F.lag("seq_no").over(w_hist).alias("prev_seq_no"),
        F.lag("prev_cost").over(w_hist).alias("prev_prev_cost"),
        F.lead("prev_cost").over(w_hist).alias("next_prev_cost"),
        F.row_number().over(w_hist).alias("seq_no_2"),
        F.sum("seq_no").over(w3).alias("running_seq_no"),
    )
)

cleaned = (
    scored_shipments
    .filter(F.col("delivered_date") >= F.to_date(F.lit("2023-08-28")))
    .withColumn("shipped_date_plus_months_label", F.date_format("shipped_date_plus_months", "yyyy-MM"))
)

result = (
    cleaned
    .groupBy("running_seq_no")
    .agg(
        F.percentile_approx("prev_cost", 0.5).alias("prev_cost_p50"),
        F.percentile_approx("prev_seq_no", [0.25, 0.5, 0.75]).alias("prev_seq_no_quartiles"),
    )
    .orderBy(F.col("prev_cost_p50").asc_nulls_first())
)
