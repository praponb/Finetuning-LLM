from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "supplier_id").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(-3, -1)
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.to_date(F.lit("2024-01-29")))
    .select(
        "*",
        F.regexp_replace("product_name", " ", "").alias("product_name_slug"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_filled"),
    )
)

not_flagged = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([7, 17, 33, 36])), "left_anti")
)

scored = (
    not_flagged
    .select(
        "*",
        F.last("brand", ignorenulls=True).over(w).alias("last_known_brand"),
        F.avg("price").over(w2).alias("price_avg_3_prior"),
    )
)

latest_match = (
    scored
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

result = (
    latest_match
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
    )
)
