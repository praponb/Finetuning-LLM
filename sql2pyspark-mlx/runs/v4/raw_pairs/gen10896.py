from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("greatest_line_total_unit_price").desc(), "id")
w2 = Window.orderBy("greatest_line_total_unit_price", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.orderBy("unit_price", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("order_id")
w5 = Window.partitionBy("order_id", "product_id").orderBy("greatest_line_total_unit_price_avg_6", "id")
w6 = Window.partitionBy("order_id").orderBy("event_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w7 = Window.orderBy("greatest_line_total_unit_price_avg_5_prior", "id").rowsBetween(-6, -1)

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
    .withColumn("line_total_2", (F.col("quantity") * F.col("unit_price")))
)

cleaned = (
    filtered
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "unit_price",
        "line_total",
        F.greatest("line_total", "unit_price").alias("greatest_line_total_unit_price"),
    )
)

enriched = (
    cleaned
    .withColumn("event_seq", F.row_number().over(w))
    .withColumn("greatest_line_total_unit_price_avg_6", F.avg("greatest_line_total_unit_price").over(w2))
    .withColumn("greatest_line_total_unit_price_avg_5_prior", F.avg("greatest_line_total_unit_price").over(w3))
)

scored = (
    enriched
    .withColumn("order_id_count_greatest_line_total_unit_price", F.count("greatest_line_total_unit_price").over(w4))
    .withColumn("prev_greatest_line_total_unit_price_avg_6", F.lag("greatest_line_total_unit_price_avg_6", 1).over(w5))
    .withColumn("first_known_greatest_line_total_unit_price_avg_5_prior", F.first("greatest_line_total_unit_price_avg_5_prior", ignorenulls=True).over(w6))
    .withColumn("greatest_line_total_unit_price_avg_6_avg_6_prior", F.avg("greatest_line_total_unit_price_avg_6").over(w7))
)

result = (
    scored
    .groupBy(
        "order_id",
        "product_id",
        F.when(F.col("greatest_line_total_unit_price_avg_6") >= 77, "HIGH").otherwise("LOW").alias("greatest_line_total_unit_price_avg_6_tier"),
    )
    .agg(
        F.max("line_total").alias("max_line_total"),
        F.percentile_approx("greatest_line_total_unit_price_avg_6_avg_6_prior", [0.25, 0.5, 0.75]).alias("greatest_line_total_unit_price_avg_6_avg_6_prior_quartiles"),
    )
    .filter(F.col("max_line_total") < 0)
)
