from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy("cost")
w2 = Window.partitionBy("order_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("carrier").orderBy("cost", "id")
w4 = Window.orderBy("cost", "id")

cleaned = (
    df_shipments
    .filter(F.col("carrier").isNotNull() & (F.col("carrier").eqNullSafe(F.lit("Kerry"))))
    .select(
        "id",
        "order_id",
        "carrier",
        "shipped_date",
        "delivered_date",
        "cost",
        F.last_day("delivered_date").alias("delivered_date_month_end"),
        F.concat_ws("/", "status", "carrier").alias("status_carrier_key"),
    )
)

with_windows = (
    cleaned
    .select(
        "id",
        "cost",
        "delivered_date_month_end",
        "status_carrier_key",
        F.rank().over(w).alias("cost_rank"),
        F.first("carrier", ignorenulls=True).over(w2).alias("first_known_carrier"),
        F.lag("cost", 1).over(w3).alias("prev_cost"),
    )
)

scored = (
    with_windows
    .filter(F.col("prev_cost") <= 47)
    .select(
        "*",
        F.lead("cost_rank", 1).over(w4).alias("next_cost_rank"),
        F.ntile(5).over(w4).alias("cost_bucket"),
    )
)

valid = (
    scored
    .filter(F.col("delivered_date_month_end") <= "2024-02-01")
    .withColumn("prev_cost_int", F.col("prev_cost").cast("int"))
)

result = (
    valid
    .select(
        "id",
        "delivered_date_month_end",
        "cost_rank",
        "first_known_carrier",
        "cost_bucket",
        "prev_cost_int",
    )
    .orderBy(F.col("cost_rank").desc())
)
