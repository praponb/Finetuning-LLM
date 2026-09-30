from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id").orderBy(F.col("shipping_fee").desc())
w2 = Window.partitionBy("customer_id", "store_id")
w3 = Window.partitionBy("customer_id").orderBy("amount", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("customer_id").orderBy("order_date", "id")
w5 = Window.partitionBy("customer_id").orderBy("order_date", "id").rowsBetween(-2, -1)

cleaned = (
    df_orders
    .filter(F.col("store_id") == 8)
    .select(
        "id",
        "customer_id",
        "store_id",
        "order_date",
        "amount",
        "discount",
        "shipping_fee",
        F.least("discount", "amount").alias("least_discount_amount"),
    )
)

kept = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.product_id").isNull()), "left_anti")
)

kept_orders = (
    kept
    .alias("s")
    .join(df_payments.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.paid_at") < F.col("s.order_date")), "left_anti")
)

scored = (
    kept_orders
    .select(
        "*",
        F.rank().over(w).alias("shipping_fee_rank"),
        F.count("shipping_fee").over(w2).alias("customer_id_count_shipping_fee"),
        F.sum("discount").over(w3).alias("running_discount"),
        F.lag("amount", 1).over(w4).alias("prev_amount"),
        F.avg("shipping_fee").over(w5).alias("shipping_fee_avg_2_prior"),
    )
)

result = (
    scored
    .groupBy(
        F.date_trunc("YEAR", "order_date").alias("year_start"),
        F.when(F.col("shipping_fee") >= 31, "HIGH").otherwise("LOW").alias("shipping_fee_tier"),
    )
    .agg(
        F.corr("shipping_fee", "amount").alias("corr_shipping_fee_amount"),
        F.count("*").alias("num_rows"),
        F.percentile_approx("amount", [0.25, 0.5, 0.75]).alias("amount_quartiles"),
        F.round(F.avg("customer_id_count_shipping_fee"), 2).alias("avg_customer_id_count_shipping_fee"),
        F.round(F.avg("prev_amount"), 2).alias("avg_prev_amount"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)
