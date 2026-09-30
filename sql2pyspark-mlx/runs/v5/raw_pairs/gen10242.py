from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("least_quantity_unit_price").asc_nulls_first(), "id")
w_part = Window.partitionBy("order_id").orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

cleaned = (
    df_order_items
    .filter(F.col("unit_price") < 220)
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

scored = (
    latest
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "dedup_rank",
        F.sum("least_quantity_unit_price").over(w_part).alias("running_sum_least_quantity_unit_price"),
        F.avg("least_quantity_unit_price").over(w_part).alias("running_avg_least_quantity_unit_price"),
        F.count("least_quantity_unit_price").over(w_part).alias("running_count_least_quantity_unit_price"),
        F.min("least_quantity_unit_price").over(w_part).alias("running_min_least_quantity_unit_price"),
    )
)

with_dims = (
    scored
    .alias("s")
    .join(df_orders.alias("d"), F.col("s.order_id") == F.col("d.id"), "left")
    .select("s.*", F.col("d.amount"), F.col("d.discount"), F.col("d.order_date"))
)

result = (
    with_dims
    .withColumn("quarter_start", F.date_trunc("QUARTER", "order_date"))
    .withColumn("year_start", F.date_trunc("YEAR", "order_date"))
    .withColumn("amount_tier", F.when(F.col("amount") >= 1520, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["quarter_start", "year_start", "amount_tier"], ["quarter_start", "year_start"], ["quarter_start"], []],
        "quarter_start",
        "year_start",
        "amount_tier",
    )
    .agg(
        F.min("unit_price").alias("min_unit_price"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("running_avg_least_quantity_unit_price", 0.9).alias("running_avg_least_quantity_unit_price_p90"),
        F.grouping("quarter_start").alias("grouping_level"),
    )
)
