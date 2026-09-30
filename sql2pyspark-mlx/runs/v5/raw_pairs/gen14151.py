from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category")
w2 = Window.partitionBy("brand").orderBy(F.col("margin_2").asc_nulls_first())
w3 = Window.partitionBy("category", "supplier_id")

cleaned = (
    df_products
    .filter((F.col("launch_date") >= F.lit("2023-07-31").cast("date")) & (F.col("cost") < 680))
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "cost",
        "stock_qty",
        "launch_date",
        "supplier_id",
        F.round((F.col("price") / F.col("cost")), 2).alias("margin"),
        (F.col("price") + F.col("cost")).alias("margin_2"),
    )
)

kept_keys = (
    cleaned
    .select("brand", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("cost") <= 710)
        .select("brand", "supplier_id")
    )
)

kept_rows = (
    cleaned
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

scored = (
    kept_rows
    .select(
        "id",
        "category",
        "cost",
        "launch_date",
        "supplier_id",
        "margin_2",
        F.count("margin_2").over(w).alias("category_count_margin_2"),
        F.dense_rank().over(w2).alias("margin_2_dense_rank"),
    )
)

scored_products = (
    scored
    .select(
        "*",
        F.round((F.col("margin_2_dense_rank") / F.sum("margin_2_dense_rank").over(w3)), 4).alias("margin_2_dense_rank_share"),
    )
)

result = (
    scored_products
    .groupBy(F.date_trunc("QUARTER", "launch_date").alias("quarter_start"), "category")
    .agg(
        F.avg(F.when(F.col("category") == "Sports", F.col("margin_2"))).alias("sports_avg_margin_2"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_rows"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .orderBy("quarter_start")
)
