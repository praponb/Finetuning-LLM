from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.partitionBy("status").orderBy("shipping_fee", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("customer_id").orderBy(F.col("amount").desc(), "id")

enriched = (
    df_orders
    .filter(F.col("amount") >= 1250)
    .select(
        "*",
        F.sum("amount").over(w_hist).alias("running_sum_amount"),
        F.count("amount").over(w_hist).alias("running_count_amount"),
        F.min("amount").over(w_hist).alias("running_min_amount"),
        F.max("amount").over(w_hist).alias("running_max_amount"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "customer_id",
        "order_date",
        "amount",
        "shipping_fee",
        "running_sum_amount",
        "running_max_amount",
    )
)

kept_keys = (
    deduped
    .select("id", "customer_id")
    .exceptAll(
        df_orders
        .filter(F.col("customer_id").isin([9, 20, 23]))
        .select("id", "customer_id")
    )
)

kept_rows = (
    deduped
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.customer_id") == F.col("k.customer_id")), "inner")
    .select("s.*")
)

not_flagged = (
    kept_rows
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.order_id") == F.col("s.id")) & (F.col("c.unit_price").between(230, 380)), "left_anti")
)

not_flagged_orders = (
    not_flagged
    .alias("s")
    .join(df_shipments.alias("ch"), (F.col("ch.order_id") == F.col("s.id")) & (F.col("ch.shipped_date") < F.col("s.order_date")), "left_anti")
)

joined = (
    not_flagged_orders
    .alias("e")
    .join(df_customers.alias("c"), F.col("e.customer_id") == F.col("c.id"), "left")
    .select(
        "e.customer_id",
        "e.shipping_fee",
        F.col("c.signup_date"),
        F.col("c.credit_limit"),
        F.col("c.country"),
    )
)

filtered = (
    joined
    .filter(F.col("country") != "JP")
    .select(
        "customer_id",
        "shipping_fee",
        "signup_date",
        "credit_limit",
        F.date_trunc("YEAR", "signup_date").alias("signup_date_period"),
        F.coalesce("country", F.lit("N/A")).alias("country_filled"),
    )
)

totals = (
    filtered
    .groupBy(
        F.date_trunc("QUARTER", "signup_date").alias("quarter_start"),
        F.when(F.col("shipping_fee") >= 44, "LARGE").otherwise("SMALL").alias("shipping_fee_tier"),
    )
    .agg(
        F.round(F.avg("shipping_fee"), 2).alias("avg_shipping_fee"),
        F.count("*").alias("row_count"),
    )
)

result = (
    totals
    .select("quarter_start", "shipping_fee_tier", "avg_shipping_fee", "row_count")
    .orderBy(F.col("quarter_start").asc_nulls_first())
)
