from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_per_unit", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(-2, -1)
w3 = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-05-15").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit_2"),
    )
)

cleaned_store_sales = (
    cleaned
    .filter((F.col("sale_date") >= F.to_date(F.lit("2023-06-11"))) & (F.col("revenue") <= 3500))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue_per_unit",
        "revenue_per_unit_2",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
    )
)

scored = (
    cleaned_store_sales
    .select(
        "id",
        "product_id",
        "revenue_per_unit",
        "revenue_per_unit_2",
        "days_since_sale_date",
        F.sum("days_since_sale_date").over(w).alias("running_days_since_sale_date"),
        F.avg("days_since_sale_date").over(w2).alias("days_since_sale_date_avg_2_prior"),
    )
)

metrics = (
    scored
    .filter(F.col("revenue_per_unit_2").between(51, 74))
    .select(
        "id",
        "product_id",
        "revenue_per_unit",
        "revenue_per_unit_2",
        "days_since_sale_date",
        "running_days_since_sale_date",
        F.lag("days_since_sale_date").over(w3).alias("prev_days_since_sale_date"),
        F.lag("days_since_sale_date_avg_2_prior").over(w3).alias("prev_days_since_sale_date_avg_2_prior"),
        F.lead("days_since_sale_date_avg_2_prior").over(w3).alias("next_days_since_sale_date_avg_2_prior"),
        F.row_number().over(w3).alias("event_seq"),
    )
)

filtered = (
    metrics
    .filter(F.col("running_days_since_sale_date").between(231, 288))
    .select(
        "id",
        "product_id",
        "revenue_per_unit",
        "revenue_per_unit_2",
        "days_since_sale_date",
        "prev_days_since_sale_date_avg_2_prior",
        "next_days_since_sale_date_avg_2_prior",
        "event_seq",
        F.round("running_days_since_sale_date", 1).alias("running_days_since_sale_date_rounded"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.revenue_per_unit",
        "a.prev_days_since_sale_date_avg_2_prior",
        "a.next_days_since_sale_date_avg_2_prior",
        F.col("p.stock_qty"),
        F.col("p.price"),
    )
)

with_dims = (
    enriched
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.prev_days_since_sale_date_avg_2_prior",
        F.col("b.stock_qty").alias("product_stock_qty"),
    )
)

result = (
    with_dims
    .groupBy(
        "product_id",
        F.when(F.col("prev_days_since_sale_date_avg_2_prior") >= 89, "LARGE").otherwise("SMALL").alias("prev_days_since_sale_date_avg_2_prior_tier"),
    )
    .agg(
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.percentile_approx("product_stock_qty", 0.5).alias("product_stock_qty_p50"),
        F.round(F.avg("prev_days_since_sale_date_avg_2_prior"), 2).alias("avg_prev_days_since_sale_date_avg_2_prior"),
        F.min("product_stock_qty").alias("min_product_stock_qty"),
    )
    .orderBy(F.col("distinct_product_ids").asc_nulls_first())
)
