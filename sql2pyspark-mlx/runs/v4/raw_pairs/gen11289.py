from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy("cost", "id")
w3 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand")
w5 = Window.orderBy("rating", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("supplier_id")

with_dims = (
    df_products
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "inner")
    .select("s.*", F.col("p.city"), F.col("p.rating"), F.col("p.supplier_name"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_suppliers.alias("p"), F.col("e.supplier_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.rating").alias("supplier_rating"))
)

with_last = (
    joined
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units").alias("store_sale_units"))
)

enriched = (
    with_last
    .select(
        "*",
        F.row_number().over(w2).alias("seq_no"),
        F.sum("stock_qty").over(w3).alias("cumulative_stock_qty"),
        F.stddev_samp("rating").over(w4).alias("brand_stddev_samp_rating"),
        F.last("brand", ignorenulls=True).over(w5).alias("last_known_brand"),
        F.round((F.col("supplier_rating") / F.sum("supplier_rating").over(w6)), 4).alias("supplier_rating_share"),
    )
)

result = (
    enriched
    .select(
        "id",
        "category",
        "price",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "city",
        "rating",
        F.when(F.col("category").rlike("^(Books|Home)$"), "EXTENDED").when(F.col("category").rlike("^(Electronics|Sports)$"), "CORE").otherwise("OTHER_ZONE").alias("category_zone"),
    )
    .orderBy(F.col("id").desc_nulls_last(), "category_zone")
    .limit(10)
)
