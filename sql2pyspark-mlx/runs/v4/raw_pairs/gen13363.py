from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("rating").asc_nulls_first())
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("city").orderBy(F.col("stock_qty").desc_nulls_last())
w4 = Window.partitionBy("city")

kept_keys = (
    df_products
    .select("category")
    .exceptAll(
        df_products
        .filter(F.col("price").isNotNull())
        .select("category")
    )
)

kept_rows = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

cleaned = (
    kept_rows
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-04-25")))
    .withColumn("days_since_launch_date", F.datediff(F.current_date(), "launch_date"))
)

kept = (
    cleaned
    .alias("s")
    .join(df_inventory.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.last_restock").between("2023-10-16", "2025-03-25")), "left_anti")
)

joined = (
    kept
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "inner")
    .select("s.*", F.col("b.city"))
)

joined_products = (
    joined
    .alias("src")
    .join(df_suppliers.alias("b"), F.col("src.supplier_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.rating"), F.col("b.supplier_name"))
)

enriched = (
    joined_products
    .withColumn("rating_cume_dist", F.cume_dist().over(w))
    .withColumn("supplier_id_avg_days_since_launch_date", F.avg("days_since_launch_date").over(w2))
    .withColumn("stock_qty_rank", F.rank().over(w3))
    .withColumn("cost_share", F.round((F.col("cost") / F.sum("cost").over(w4)), 4))
)

joined_products_2 = (
    enriched
    .alias("s")
    .join(df_suppliers.alias("d"), F.col("s.supplier_id") == F.col("d.id"), "inner")
    .select(
        "s.*",
        F.col("d.supplier_name").alias("supplier_supplier_name"),
        F.col("d.city").alias("supplier_city"),
        F.col("d.rating").alias("supplier_rating"),
    )
)

stats = (
    joined_products_2
    .groupBy(F.date_trunc("YEAR", "launch_date").alias("year_start"))
    .agg(
        F.sum("days_since_launch_date").alias("total_days_since_launch_date"),
        F.countDistinct("city").alias("distinct_cities"),
        F.count(F.when(F.col("brand") == "Globex", 1)).alias("globex_count"),
        F.sum(F.when(F.col("supplier_city") == "Berlin", F.col("supplier_id_avg_days_since_launch_date"))).alias("berlin_total_supplier_id_avg_days_since_launch_date"),
    )
)

result = (
    stats
    .select(
        "year_start",
        "total_days_since_launch_date",
        "distinct_cities",
        "berlin_total_supplier_id_avg_days_since_launch_date",
    )
)
