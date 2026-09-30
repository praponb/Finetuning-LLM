from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id")
w2 = Window.partitionBy("status", "order_id").orderBy("corr_quantity_unit_price", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("status").orderBy(F.col("corr_quantity_unit_price").desc(), "id")

enriched = (
    df_order_items
    .withColumn("corr_quantity_unit_price", F.corr("quantity", "unit_price").over(w))
    .withColumn("order_id_stddev_samp_quantity", F.stddev_samp("quantity").over(w))
)

cleaned = (
    enriched
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "corr_quantity_unit_price",
        (F.col("quantity") + F.col("unit_price")).alias("line_total"),
    )
)

enriched_order_items = (
    cleaned
    .alias("e")
    .join(df_orders.alias("b"), F.col("e.order_id") == F.col("b.id"), "left")
    .select(
        "e.id",
        "e.order_id",
        "e.corr_quantity_unit_price",
        "e.line_total",
        F.col("b.status"),
    )
)

filtered = (
    enriched_order_items
    .filter(F.col("status") == "SHIPPED")
    .select(
        "id",
        "order_id",
        "corr_quantity_unit_price",
        "status",
        F.round("line_total", 2).alias("line_total_rounded"),
        F.abs("corr_quantity_unit_price").alias("abs_corr_quantity_unit_price"),
    )
)

scored = (
    filtered
    .select(
        "id",
        "order_id",
        "corr_quantity_unit_price",
        "status",
        "line_total_rounded",
        "abs_corr_quantity_unit_price",
        F.avg("abs_corr_quantity_unit_price").over(w2).alias("last3_avg_abs_corr_quantity_unit_price"),
        F.max("abs_corr_quantity_unit_price").over(w2).alias("last3_max_abs_corr_quantity_unit_price"),
        F.sum("abs_corr_quantity_unit_price").over(w2).alias("last3_sum_abs_corr_quantity_unit_price"),
        F.ntile(5).over(w3).alias("corr_quantity_unit_price_bucket"),
    )
)

remaining_keys = (
    scored
    .select("order_id", "id")
    .subtract(
        df_order_items
        .filter(F.col("order_id").isin([13, 16]))
        .select("order_id", "id")
    )
)

verified = (
    scored
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.order_id") == F.col("k.order_id")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .select(
        "status",
        "last3_sum_abs_corr_quantity_unit_price",
        "last3_max_abs_corr_quantity_unit_price",
    )
    .unpivot(
        ids=["status"],
        values=["last3_sum_abs_corr_quantity_unit_price", "last3_max_abs_corr_quantity_unit_price"],
        variableColumnName="metric",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
