from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.revenue").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("category", "supplier_id").orderBy(F.col("cost").desc_nulls_last())
w3 = Window.partitionBy("brand", "category").orderBy("store_sale_units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("category")
w6 = Window.partitionBy("brand")
stock_qty_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("stock_qty").alias("sum_product_stock_qty"))

base = (
    df_products
    .filter((F.col("launch_date") > F.lit("2023-11-21").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        F.upper("category").alias("category_upper"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_filled"),
    )
)

latest_match = (
    base
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

deduped = (
    latest_match
    .withColumn("row_num", F.rank().over(w2))
    .filter(F.col("row_num") == 1)
)

enriched = (
    deduped
    .select(
        "*",
        F.sum("stock_qty").over(w3).alias("running_stock_qty"),
        F.sum("price").over(w4).alias("running_price"),
        F.count("store_sale_units").over(w5).alias("category_count_store_sale_units"),
        F.round((F.col("store_sale_units") / F.sum("store_sale_units").over(w6)), 4).alias("store_sale_units_share"),
    )
)

result = (
    enriched
    .join(stock_qty_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.col("sum_product_stock_qty"),
    )
    .orderBy(F.col("sum_product_stock_qty").desc_nulls_first())
)
