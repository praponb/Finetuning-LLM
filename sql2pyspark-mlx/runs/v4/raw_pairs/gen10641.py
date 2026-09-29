from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

latest_match = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

result = (
    latest_match
    .cube(
        F.when(F.col("price") >= 1140, "HIGH").otherwise("LOW").alias("price_tier"),
        F.date_trunc("quarter", "launch_date").alias("quarter_start"),
    )
    .agg(
        F.round(F.avg("cost"), 2).alias("avg_cost"),
        F.percentile_approx("stock_qty", 0.9).alias("stock_qty_p90"),
        F.count_if(F.col("category") == "Grocery").alias("grocery_events"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .orderBy(F.col("quarter_start").desc())
)
