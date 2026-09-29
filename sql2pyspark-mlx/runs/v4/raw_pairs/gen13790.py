from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("unit_price").desc_nulls_last())
w2 = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("order_id").orderBy("rn", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("product_id", "order_id").orderBy("unit_price_share", "id")
w_recent = Window.partitionBy("order_id").orderBy("unit_price", "id")

latest = (
    df_order_items
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

scored = (
    latest
    .select(
        "*",
        F.percent_rank().over(w).alias("unit_price_percent_rank"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share"),
        F.avg("unit_price").over(w3).alias("unit_price_avg_2_prior"),
        F.sum("quantity").over(w4).alias("cumulative_quantity"),
        F.round((F.col("unit_price") / F.sum("unit_price").over(w2)), 4).alias("unit_price_share_2"),
    )
)

with_windows = (
    scored
    .withColumn("prev_unit_price_share", F.lag("unit_price_share", 1).over(w5))
    .withColumn("seq_no", F.row_number().over(w_recent))
)

result = (
    with_windows
    .select(
        "id",
        "product_id",
        "unit_price",
        "prev_unit_price_share",
        "unit_price_share_2",
        "unit_price_percent_rank",
    )
    .unpivot(
        ["id", "product_id"],
        ["unit_price", "prev_unit_price_share", "unit_price_share_2", "unit_price_percent_rank"],
        "kpi",
        "value",
    )
)
