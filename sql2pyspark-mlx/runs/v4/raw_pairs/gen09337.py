from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("carrier", "order_id").orderBy(F.col("cost").desc_nulls_first())
w2 = Window.partitionBy("status").orderBy("shipped_date", "id")
w3 = Window.orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("order_id").orderBy("delivered_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("carrier").orderBy(F.col("cumulative_cost").desc_nulls_last(), "id")
w6 = Window.partitionBy("carrier").orderBy("prev_cost_int", "id")
w7 = Window.partitionBy("order_id").orderBy(F.col("prev_cost").asc_nulls_last())
w8 = Window.orderBy("cost_percent_rank_filled", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("cumulative_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
unit_price_by_order_id = df_order_items.groupBy("order_id").agg(F.sum("unit_price").alias("sum_order_item_unit_price"))

scored = (
    df_shipments
    .withColumn("cost_percent_rank", F.percent_rank().over(w))
    .withColumn("prev_cost", F.lag("cost", 2).over(w2))
    .withColumn("cumulative_cost", F.sum("cost").over(w3))
    .withColumn("running_cost", F.sum("cost").over(w4))
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w5))
    .filter(F.col("latest_rank") == 1)
)

base = (
    ranked
    .filter(F.col("carrier").isNotNull() & F.col("running_cost").between(158, 474))
    .withColumn("cost_percent_rank_filled", F.coalesce("cost_percent_rank", F.lit(0)))
    .withColumn("prev_cost_int", F.col("prev_cost").cast("int"))
)

latest = (
    base
    .withColumn("rn", F.row_number().over(w6))
    .filter(F.col("rn") == 1)
)

scored_shipments = (
    latest
    .withColumn("prev_cost_rank", F.rank().over(w7))
    .withColumn("last_known_cost_percent_rank_filled", F.last("cost_percent_rank_filled", ignorenulls=True).over(w8))
    .withColumn("last_known_cost_percent_rank_filled_2", F.last("cost_percent_rank_filled", ignorenulls=True).over(w9))
)

result = (
    scored_shipments
    .join(unit_price_by_order_id, on="order_id", how="left")
    .select(
        "id",
        "order_id",
        "shipped_date",
        "delivered_date",
        "cost",
        "prev_cost",
        "latest_rank",
        "rn",
        "prev_cost_rank",
        F.col("sum_order_item_unit_price"),
    )
    .orderBy(F.col("prev_cost").desc(), "cost")
)
