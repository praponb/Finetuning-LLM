from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_hist = Window.orderBy("quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_hist_2 = Window.partitionBy("product_id").orderBy("unit_price", "id").rowsBetween(-6, -1)
w3 = Window.partitionBy("order_id")
w4 = Window.partitionBy("order_id").orderBy("least_quantity_unit_price", "id").rowsBetween(-6, Window.currentRow)
w5 = Window.orderBy("least_quantity_unit_price_avg_6", "id").rowsBetween(-6, Window.currentRow)
w6 = Window.orderBy("last7_max_corr_quantity_unit_price", "id")

cleaned = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "unit_price",
        F.least("quantity", "unit_price").alias("least_quantity_unit_price"),
    )
)

scored = (
    cleaned
    .select(
        "id",
        "unit_price",
        "least_quantity_unit_price",
        F.sum("unit_price").over(w_hist).alias("running_unit_price"),
        F.avg("quantity").over(w_hist_2).alias("quantity_avg_6_prior"),
        F.corr("quantity", "unit_price").over(w3).alias("corr_quantity_unit_price"),
        F.avg("least_quantity_unit_price").over(w4).alias("least_quantity_unit_price_avg_6"),
    )
)

enriched = (
    scored
    .select(
        "*",
        F.avg("corr_quantity_unit_price").over(w5).alias("last7_avg_corr_quantity_unit_price"),
        F.count("corr_quantity_unit_price").over(w5).alias("last7_count_corr_quantity_unit_price"),
        F.min("corr_quantity_unit_price").over(w5).alias("last7_min_corr_quantity_unit_price"),
        F.max("corr_quantity_unit_price").over(w5).alias("last7_max_corr_quantity_unit_price"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "least_quantity_unit_price",
        "running_unit_price",
        "quantity_avg_6_prior",
        "corr_quantity_unit_price",
        "least_quantity_unit_price_avg_6",
        "last7_avg_corr_quantity_unit_price",
        "last7_min_corr_quantity_unit_price",
        "last7_max_corr_quantity_unit_price",
        F.lead("last7_count_corr_quantity_unit_price").over(w6).alias("next_last7_count_corr_quantity_unit_price"),
        F.lag("last7_count_corr_quantity_unit_price").over(w6).alias("prev_last7_count_corr_quantity_unit_price"),
        F.lead("last7_min_corr_quantity_unit_price").over(w6).alias("next_last7_min_corr_quantity_unit_price"),
        F.lag("last7_min_corr_quantity_unit_price").over(w6).alias("prev_last7_min_corr_quantity_unit_price"),
        F.row_number().over(w6).alias("seq_no"),
    )
)

daily = (
    metrics
    .groupBy(
        F.when(F.col("running_unit_price") >= 2109, "PREMIUM").otherwise("STANDARD").alias("running_unit_price_tier"),
    )
    .agg(
        F.count("*").alias("row_count"),
        F.avg("quantity_avg_6_prior").alias("avg_quantity_avg_6_prior"),
        F.count("*").alias("event_count"),
        F.percentile_approx("running_unit_price", 0.5).alias("running_unit_price_p50"),
        F.percentile_approx("last7_avg_corr_quantity_unit_price", 0.5).alias("last7_avg_corr_quantity_unit_price_p50"),
    )
    .filter(F.col("avg_quantity_avg_6_prior") > 0)
)

result = (
    daily
    .select(
        "running_unit_price_tier",
        "row_count",
        "avg_quantity_avg_6_prior",
        "running_unit_price_p50",
    )
)
