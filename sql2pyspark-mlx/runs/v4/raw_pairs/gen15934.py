from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy("days_since_launch_date", "id").rowsBetween(-5, -1)
w_grp = Window.partitionBy("supplier_id").orderBy("cost", "id").rowsBetween(-6, Window.currentRow)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

base = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2024-02-22")))
    .select(
        "id",
        "product_name",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.lower("product_name").alias("product_name_lower"),
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("N/A")).alias("brand_clean"),
    )
)

scored = (
    base
    .withColumn("stock_qty_avg_5_prior", F.avg("stock_qty").over(w))
    .withColumn("cost_avg_6", F.avg("cost").over(w_grp))
)

kept = (
    scored
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.year(F.col("x.last_restock")) >= 2024), "left_anti")
)

enriched = (
    kept
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "inner")
    .select("a.*", F.col("b.rating"), F.col("b.supplier_name"))
)

with_last = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

filtered = (
    with_last
    .filter((F.col("launch_date") >= F.lit("2023-09-02").cast("date")) & (F.year("launch_date") == 2024))
    .withColumn("brand_product_name", F.concat("brand", F.lit(" "), "product_name"))
    .withColumn("launch_date_month_end", F.last_day("launch_date"))
    .withColumn("resolved_brand", F.coalesce("brand", F.lit("N/A")))
)

result = (
    filtered
    .groupBy("brand", F.date_trunc("year", "launch_date_month_end").alias("year_start"))
    .agg(
        F.sum(F.when(F.col("brand") == "Acme", F.col("days_since_launch_date")).otherwise(0)).alias("acme_days_since_launch_date"),
        F.round(F.avg("stock_qty_avg_5_prior"), 2).alias("avg_stock_qty_avg_5_prior"),
        F.percentile_approx("cost_avg_6", 0.95).alias("cost_avg_6_p95"),
        F.count_if(F.col("brand") == "Globex").alias("globex_events"),
    )
    .orderBy(F.col("globex_events").desc_nulls_first())
)
