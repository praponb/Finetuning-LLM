from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("order_id", "product_id").orderBy(F.col("line_total_plus_unit_price").desc(), "id")
w2 = Window.partitionBy("brand").orderBy("quantity", "id")
w_recent = Window.partitionBy("product_id").orderBy("line_total", "id")
w_part = Window.partitionBy("order_id", "brand").orderBy("line_total", "id")
w5 = Window.orderBy("quantity_str", "id").rowsBetween(-3, -1)
amount_by_order_id = df_payments.groupBy("order_id").agg(F.sum("amount").alias("sum_payment_amount"))

valid = (
    df_order_items
    .filter(F.col("unit_price") >= 370)
    .withColumn("line_total", (F.col("unit_price") - F.col("quantity")))
)

filtered = (
    valid
    .filter(F.col("product_id").isNotNull())
    .select(
        "*",
        F.col("quantity").cast("double").alias("quantity_str"),
        (F.col("line_total") + F.col("unit_price")).alias("line_total_plus_unit_price"),
    )
)

deduped = (
    filtered
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

enriched = (
    deduped
    .alias("e")
    .join(df_products.alias("d"), F.col("e.product_id") == F.col("d.id"), "inner")
    .select("e.*", F.col("d.brand"), F.col("d.product_name"), F.col("d.price"))
)

enriched_order_items = (
    enriched
    .select(
        "*",
        F.lag("price", 2).over(w2).alias("prev_price"),
        F.lead("quantity_str", 1).over(w_recent).alias("next_quantity_str"),
        F.lag("line_total", 1).over(w_part).alias("prev_line_total"),
        F.avg("line_total").over(w5).alias("line_total_avg_3_prior"),
    )
)

enriched_order_items_2 = (
    enriched_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.category"))
)

stats = (
    enriched_order_items_2
    .cube("order_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("order_id").alias("distinct_order_ids"),
        F.bit_and("quantity").alias("quantity_bit_and"),
        F.percentile_approx("next_quantity_str", [0.25, 0.5, 0.75]).alias("next_quantity_str_quartiles"),
    )
)

result = (
    stats
    .join(amount_by_order_id, on="order_id", how="left")
    .select(
        "order_id",
        "row_count",
        "distinct_order_ids",
        "quantity_bit_and",
        "next_quantity_str_quartiles",
        F.col("sum_payment_amount"),
    )
    .orderBy("sum_payment_amount")
)
