from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("category").orderBy("launch_date_month_end", "id")
w3 = Window.orderBy(F.col("adjusted_price").desc(), "id")
w4 = Window.partitionBy("country").orderBy("prev_adjusted_price_plus_adjusted_price", "id")

filtered = (
    df_products
    .filter((F.col("launch_date") >= F.to_date(F.lit("2023-05-06"))) & F.col("supplier_id").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "stock_qty",
        "tags",
        "supplier_id",
        F.round((F.col("price") * 100), 2).alias("adjusted_price"),
        F.last_day("launch_date").alias("launch_date_month_end"),
    )
)

enriched = (
    filtered
    .alias("e")
    .join(df_suppliers.alias("d"), F.col("e.supplier_id") == F.col("d.id"), "left")
    .filter(~F.col("d.supplier_name").like("T%"))
    .select("e.*", F.col("d.country"), F.col("d.city"))
)

enriched_products = (
    enriched
    .select(
        "id",
        "category",
        "stock_qty",
        "adjusted_price",
        "launch_date_month_end",
        "country",
        F.min("adjusted_price").over(w).alias("city_min_adjusted_price"),
        F.lag("adjusted_price", 2).over(w2).alias("prev_adjusted_price"),
        F.row_number().over(w3).alias("row_seq"),
    )
)

valid = (
    enriched_products
    .filter(F.col("stock_qty") > 176)
    .select(
        "id",
        "category",
        "stock_qty",
        "adjusted_price",
        "launch_date_month_end",
        "country",
        "city_min_adjusted_price",
        "prev_adjusted_price",
        (F.col("prev_adjusted_price") + F.col("adjusted_price")).alias("prev_adjusted_price_plus_adjusted_price"),
        F.coalesce("country", F.lit("N/A")).alias("country_clean"),
    )
)

ranked = (
    valid
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
)

not_flagged = (
    ranked
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date_month_end")), "left_anti")
)

kept = (
    not_flagged
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock") >= F.col("s.launch_date_month_end")), "left_anti")
)

result = (
    kept
    .rollup("country")
    .agg(
        F.size(F.collect_set("category")).alias("num_categories"),
        F.count(F.when(F.col("category") == "Books", 1)).alias("books_count"),
        F.sum(F.when(F.col("category") == "Home", F.col("stock_qty")).otherwise(0)).alias("home_stock_qty"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
    )
    .filter(F.col("home_stock_qty") > 0)
    .orderBy("books_count")
)
