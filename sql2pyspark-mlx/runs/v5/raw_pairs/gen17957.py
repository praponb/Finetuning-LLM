from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy("unit_price", "id").rowsBetween(-6, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("running_unit_price", "id")
w4 = Window.partitionBy("product_id")
w_recent = Window.partitionBy("product_id").orderBy("unit_price_avg_6", "id")
w_hist = Window.orderBy("unit_price_avg_6", "id").rowsBetween(-6, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price") <= 330)
    .select("*", (F.col("quantity") * F.col("unit_price")).alias("line_total"))
)

metrics = (
    cleaned
    .filter(F.col("line_total") > 55)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.avg("unit_price").over(w).alias("unit_price_avg_6"),
        F.sum("unit_price").over(w2).alias("running_unit_price"),
    )
)

with_windows = (
    metrics
    .select(
        "id",
        "unit_price_avg_6",
        "running_unit_price",
        F.row_number().over(w3).alias("row_seq"),
        F.round((F.col("running_unit_price") / F.sum("running_unit_price").over(w4)), 4).alias("running_unit_price_share"),
        F.row_number().over(w_recent).alias("row_seq_2"),
    )
)

enriched = (
    with_windows
    .select(
        "id",
        "unit_price_avg_6",
        "running_unit_price",
        "row_seq",
        F.max("row_seq").over(w_hist).alias("last7_max_row_seq"),
        F.min("row_seq").over(w_hist).alias("last7_min_row_seq"),
        F.sum("row_seq").over(w_hist).alias("last7_sum_row_seq"),
    )
)

result = (
    enriched
    .select(
        "id",
        F.col("last7_sum_row_seq").alias("LAST7_SUM_ROW_SEQ_VAL"),
        "last7_max_row_seq",
        F.col("last7_min_row_seq").alias("LAST7_MIN_ROW_SEQ"),
    )
    .unpivot(
        ids=["id"],
        values=["LAST7_SUM_ROW_SEQ_VAL", "last7_max_row_seq", "LAST7_MIN_ROW_SEQ"],
        variableColumnName="kpi",
        valueColumnName="metric_value",
    )
    .filter(F.col("metric_value").isNotNull())
)
