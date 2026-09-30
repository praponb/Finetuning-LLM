from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("price", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("country").orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_country").orderBy("price", "id")
w4 = Window.partitionBy("brand")
w5 = Window.partitionBy("country").orderBy(F.col("rating").desc_nulls_last())

enriched = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.country"))
)

joined = (
    enriched
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.country").alias("supplier_country"), F.col("d.rating"))
)

not_flagged = (
    joined
    .alias("s")
    .join(df_order_items.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.unit_price") >= 150), "left_anti")
)

result = (
    not_flagged
    .select(
        "*",
        F.avg("stock_qty").over(w).alias("stock_qty_avg_3"),
        F.sum("rating").over(w2).alias("running_rating"),
        F.row_number().over(w3).alias("seq_no"),
        F.corr("stock_qty", "cost").over(w4).alias("corr_stock_qty_cost"),
        F.cume_dist().over(w5).alias("rating_cume_dist"),
    )
)
