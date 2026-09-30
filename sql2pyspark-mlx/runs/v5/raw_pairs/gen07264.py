from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand", "supplier_id").orderBy("cost", "id").rowsBetween(-5, Window.currentRow)
w2 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter((~F.col("brand").eqNullSafe(F.lit("Acme"))) & (F.col("price") < 680))
    .withColumn("price_per_stock_qty", (F.col("price") / F.nullif("stock_qty", F.lit(0))))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("NONE")))
)

with_dims = (
    base
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.supplier_name"), F.col("p.rating"))
)

scored = (
    with_dims
    .filter(F.col("price") > 690)
    .withColumn("price_avg_5", F.avg("price").over(w))
    .withColumn("last_known_price_per_stock_qty", F.last("price_per_stock_qty", ignorenulls=True).over(w2))
)

eligible = (
    scored
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") < F.col("s.launch_date")), "left_anti")
)

totals = (
    eligible
    .groupBy("brand", F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.count("*").alias("cnt"),
    )
)

result = totals.select("brand", "year_start", "category_list", "stock_qty_bit_or", "cnt")
