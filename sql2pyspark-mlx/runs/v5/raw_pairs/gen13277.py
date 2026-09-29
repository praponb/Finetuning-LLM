from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("country", "city").orderBy("rating", "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w3 = Window.partitionBy("country").orderBy(F.col("product_stock_qty").desc_nulls_last())
w4 = Window.partitionBy("country").orderBy(F.col("product_stock_qty").desc(), "id")
w5 = Window.partitionBy("country")
w6 = Window.partitionBy("country").orderBy("product_stock_qty", "id")
w7 = Window.orderBy("product_stock_qty", "id").rowsBetween(-3, Window.currentRow)

latest = (
    df_suppliers
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

with_latest = (
    latest
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_name",
        "s.country",
        F.col("c.stock_qty").alias("product_stock_qty"),
    )
)

ranked = (
    with_latest
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
    .select("id", "supplier_name", "country", "product_stock_qty")
)

enriched = (
    ranked
    .select(
        "id",
        "country",
        "product_stock_qty",
        F.row_number().over(w4).alias("seq_no"),
        F.min("product_stock_qty").over(w5).alias("country_min_product_stock_qty"),
        F.ntile(10).over(w4).alias("product_stock_qty_bucket"),
        F.row_number().over(w6).alias("seq_no_2"),
        F.avg("product_stock_qty").over(w7).alias("product_stock_qty_avg_3"),
    )
)

result = (
    enriched
    .select(
        "id",
        "product_stock_qty",
        "seq_no",
        "country_min_product_stock_qty",
        "product_stock_qty_bucket",
        "seq_no_2",
    )
)
