from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id").orderBy(F.col("amount").desc())
cost_by_order_id = df_shipments.groupBy("order_id").agg(F.sum("cost").alias("sum_shipment_cost"))

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("quantity") >= 5))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        (F.col("unit_price") * F.col("quantity")).alias("line_total"),
    )
)

with_dims = (
    valid
    .alias("e")
    .join(df_orders.alias("d"), F.col("e.order_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.amount"), F.col("d.order_date"))
)

ranked = (
    with_dims
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
)

grouped = (
    ranked
    .groupBy(
        F.when(F.col("line_total") >= 87, "LARGE").otherwise("SMALL").alias("line_total_tier"),
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        "order_id",
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", 0.95).alias("quantity_p95"),
        F.corr("quantity", "unit_price").alias("corr_quantity_unit_price"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .filter(F.col("quantity_p95") >= 0)
)

result = (
    grouped
    .join(cost_by_order_id, on="order_id", how="left")
    .select(
        "line_total_tier",
        "year_start",
        "order_id",
        "distinct_ids",
        "quantity_p95",
        "corr_quantity_unit_price",
        F.col("sum_shipment_cost"),
    )
    .orderBy(F.col("order_id").desc_nulls_last())
)
