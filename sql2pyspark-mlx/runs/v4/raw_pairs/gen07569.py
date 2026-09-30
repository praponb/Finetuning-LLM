from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy(F.col("price_times_stock_qty").asc_nulls_first(), "id")
w2 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("launch_date") >= F.lit("2023-05-29").cast("date"))
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.concat("brand", F.lit(" "), "product_name").alias("brand_product_name"),
        F.date_sub("launch_date", 61).alias("launch_date_minus_61d"),
    )
)

valid = (
    cleaned
    .filter((F.col("launch_date") >= F.lit("2023-04-10").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "brand_product_name",
        "launch_date_minus_61d",
        F.array_contains("tags", "sale").alias("is_sale"),
        (F.col("price") * F.col("stock_qty")).alias("price_times_stock_qty"),
        F.coalesce("brand", F.lit("OTHER")).alias("resolved_brand"),
    )
)

deduped = (
    valid
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

with_latest = (
    deduped
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "inner")
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.quantity").alias("inventory_quantity"),
        F.col("c.last_restock"),
    )
)

not_flagged = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([2, 4, 6])), "left_anti")
)

with_last = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select(
        "s.*",
        F.col("c.last_restock").alias("inventory_last_restock"),
        F.col("c.quantity"),
    )
)

result = (
    with_last
    .groupBy(F.date_trunc("month", "launch_date").alias("activity_month"))
    .agg(
        F.percentile_approx("cost", 0.9).alias("cost_p90"),
        F.count_if(F.col("category") == "Grocery").alias("grocery_events"),
        F.bit_or("stock_qty").alias("stock_qty_bit_or"),
        F.sum(F.when(F.col("brand") == "Initech", F.col("stock_qty")).otherwise(0)).alias("initech_stock_qty"),
        F.avg("stock_qty").alias("avg_stock_qty"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .filter(F.col("stock_qty_bit_or") < 1)
)
