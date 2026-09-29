from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("carrier").orderBy(F.col("cost_whole").desc_nulls_last())
w_grp = Window.partitionBy("status").orderBy("cumulative_cost_whole", "id")
w4 = Window.partitionBy("carrier", "status").orderBy(F.col("shipped_date_plus_59d").desc(), "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & F.month("shipped_date").isin([1, 2, 10]))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        "status",
        F.floor("cost").alias("cost_whole"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled"),
    )
)

cleaned_shipments = (
    cleaned
    .filter((F.col("delivered_date") >= F.to_date(F.lit("2023-09-09"))) & F.col("order_id").isNotNull())
    .select(
        "id",
        "order_id",
        "carrier",
        "delivered_date",
        "status",
        "cost_whole",
        "carrier_filled",
        F.date_add("shipped_date", 59).alias("shipped_date_plus_59d"),
        F.coalesce("carrier", F.lit("DIRECT")).alias("carrier_filled_2"),
    )
)

with_windows = (
    cleaned_shipments
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "cost_whole",
        "shipped_date_plus_59d",
        F.sum("cost_whole").over(w_hist).alias("cumulative_cost_whole"),
        F.first("cost_whole", ignorenulls=True).over(w_hist).alias("first_known_cost_whole"),
        F.rank().over(w2).alias("cost_whole_rank"),
    )
)

with_windows_shipments = (
    with_windows
    .withColumn("prev_cumulative_cost_whole", F.lag("cumulative_cost_whole").over(w_grp))
    .withColumn("prev_first_known_cost_whole", F.lag("first_known_cost_whole").over(w_grp))
    .withColumn("seq_no", F.row_number().over(w_grp))
)

filtered = (
    with_windows_shipments
    .filter(F.col("carrier").isNotNull() & F.col("cost_whole").between(30, 58))
    .select(
        "id",
        "carrier",
        "delivered_date",
        "status",
        "shipped_date_plus_59d",
        "cumulative_cost_whole",
        "prev_cumulative_cost_whole",
        "seq_no",
        (F.col("prev_first_known_cost_whole") - F.col("first_known_cost_whole")).alias("prev_first_known_cost_whole_minus_first_known_cost_whole"),
        F.coalesce("seq_no", F.lit(0)).alias("seq_no_filled"),
    )
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

result = (
    latest
    .groupBy(
        F.date_trunc("YEAR", "delivered_date").alias("year_start"),
        F.date_trunc("MONTH", "shipped_date_plus_59d").alias("activity_month"),
    )
    .agg(
        F.sum(F.when(F.col("carrier") == "UPS", F.col("cumulative_cost_whole")).otherwise(0)).alias("ups_cumulative_cost_whole"),
        F.count(F.when(F.col("carrier") == "DHL", 1)).alias("dhl_rows"),
    )
    .orderBy("dhl_rows")
)
