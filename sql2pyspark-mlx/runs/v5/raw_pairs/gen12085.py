from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand")
w2 = Window.partitionBy("supplier_id").orderBy("price", "id").rowsBetween(-2, Window.currentRow)

with_windows = (
    df_products
    .filter(F.length("product_name") >= 7)
    .select(
        "*",
        F.corr("price", "cost").over(w).alias("corr_price_cost"),
        F.avg("price").over(w2).alias("price_avg_2"),
    )
)

result = (
    with_windows
    .groupBy(
        F.when(F.col("price_avg_2") >= 979, "PREMIUM").otherwise("STANDARD").alias("price_avg_2_tier"),
        F.date_trunc("WEEK", "launch_date").alias("week_start"),
    )
    .agg(
        F.countDistinct("supplier_id").alias("distinct_supplier_ids"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.sum(F.when(F.col("category") == "Grocery", F.col("price")).otherwise(0)).alias("grocery_price"),
        F.round(F.avg("stock_qty"), 2).alias("avg_stock_qty"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .orderBy("price_avg_2_tier")
)
