from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w_part = Window.partitionBy("product_id")
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy("days_since_sale_date", "id").rowsBetween(-2, -1)
w6 = Window.partitionBy("store_id").orderBy(F.col("greatest_units_revenue").desc(), "id")
w_hist = Window.partitionBy("city", "store_id").orderBy("greatest_units_revenue_avg_2_prior", "id").rowsBetween(-5, -1)
w_running = Window.partitionBy("store_id", "city").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w9 = Window.orderBy("greatest_units_revenue_avg_2_prior", "id")

base = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-23").cast("date")) & (F.col("units") > 36))
    .withColumn("greatest_units_revenue", F.greatest("units", "revenue"))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

enriched = (
    base
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "left")
    .select("e.*", F.col("p.cost"), F.col("p.stock_qty"))
)

enriched_store_sales = (
    enriched
    .withColumn("units_bucket", F.ntile(5).over(w))
    .withColumn("product_id_stddev_samp_stock_qty", F.stddev_samp("stock_qty").over(w_part))
    .withColumn("event_seq", F.row_number().over(w3))
    .withColumn("first_known_greatest_units_revenue", F.first("greatest_units_revenue", ignorenulls=True).over(w4))
    .withColumn("greatest_units_revenue_avg_2_prior", F.avg("greatest_units_revenue").over(w5))
)

joined = (
    enriched_store_sales
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"), F.col("b.opened_date"))
)

ranked = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w6))
    .filter(F.col("rank_in_group") == 1)
)

enriched_store_sales_2 = (
    ranked
    .select(
        "*",
        F.avg("cost").over(w_hist).alias("cost_avg_5_prior"),
        F.last("greatest_units_revenue_avg_2_prior", ignorenulls=True).over(w_running).alias("last_known_greatest_units_revenue_avg_2_prior"),
        F.ntile(3).over(w9).alias("greatest_units_revenue_avg_2_prior_bucket"),
    )
)

result = (
    enriched_store_sales_2
    .groupBy(F.date_trunc("YEAR", "sale_date").alias("year_start"))
    .agg(
        F.bit_and("units").alias("units_bit_and"),
        F.percentile_approx("stock_qty", [0.25, 0.5, 0.75]).alias("stock_qty_quartiles"),
        F.round(F.sum("last_known_greatest_units_revenue_avg_2_prior") / F.nullif(F.count(F.when(F.col("city") == "Berlin", 1)), F.lit(0)), 2).alias("last_known_greatest_units_revenue_avg_2_prior_per_berlin"),
        F.sum(F.when(F.col("city") == "Bangkok", F.col("days_since_sale_date")).otherwise(0)).alias("bangkok_days_since_sale_date"),
    )
    .orderBy(F.col("year_start").desc_nulls_first())
)
