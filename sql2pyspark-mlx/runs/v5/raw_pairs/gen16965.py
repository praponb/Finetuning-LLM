from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_recent = Window.orderBy("product_cost", "id").rowsBetween(-3, Window.currentRow)

with_last = (
    df_suppliers
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.id", "s.supplier_name", F.col("c.cost").alias("product_cost"))
)

metrics = (
    with_last
    .filter(F.upper("supplier_name").like("%URO%"))
    .select(
        "id",
        "product_cost",
        F.count("product_cost").over(w_recent).alias("last4_count_product_cost"),
        F.min("product_cost").over(w_recent).alias("last4_min_product_cost"),
        F.max("product_cost").over(w_recent).alias("last4_max_product_cost"),
        F.sum("product_cost").over(w_recent).alias("last4_sum_product_cost"),
    )
)

matched_keys = (
    metrics
    .select("id")
    .intersectAll(
        df_suppliers
        .filter(F.col("country") == "TH")
        .select("id")
    )
)

reconciled = (
    metrics
    .alias("s")
    .join(matched_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

kept = (
    reconciled
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.cost").between(310, 570)), "left_anti")
)

daily = (
    kept
    .groupBy(
        F.when(F.col("last4_count_product_cost") >= 15, "LARGE").otherwise("SMALL").alias("last4_count_product_cost_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.count("*").alias("cnt"),
        F.round(F.avg("last4_max_product_cost"), 2).alias("avg_last4_max_product_cost"),
        F.round(F.avg("last4_max_product_cost"), 2).alias("avg_last4_max_product_cost_2"),
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("product_cost", 0.9).alias("product_cost_p90"),
    )
)

result = (
    daily
    .select(
        "last4_count_product_cost_tier",
        "cnt",
        "avg_last4_max_product_cost",
        "avg_last4_max_product_cost_2",
        "distinct_ids",
        "product_cost_p90",
    )
    .orderBy(F.col("product_cost_p90").desc())
)
