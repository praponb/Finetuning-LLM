from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "brand").orderBy("cost")

kept = (
    df_products
    .alias("s")
    .join(df_store_sales.alias("ch"), (F.col("ch.product_id") == F.col("s.id")) & (F.col("ch.sale_date") >= F.col("s.launch_date")), "left_anti")
)

latest = (
    kept
    .withColumn("latest_rank", F.rank().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

enriched = (
    latest
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .filter(F.col("b.country").isin(["TH", "JP"]))
    .select(
        "e.id",
        "e.brand",
        "e.cost",
        "e.launch_date",
        "e.tags",
        "e.supplier_id",
        F.col("b.country"),
    )
)

kept_keys = (
    enriched
    .select("id", "supplier_id")
    .exceptAll(
        df_products
        .filter(F.upper("product_name").like("%OGA%"))
        .select("id", "supplier_id")
    )
)

reconciled = (
    enriched
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

cleaned = (
    reconciled
    .filter(F.col("brand") == "Stark")
    .select(
        "*",
        F.lower("brand").alias("brand_lower"),
        F.coalesce("country", F.lit("NONE")).alias("resolved_country"),
    )
)

kept_products = (
    cleaned
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.unit_price") < 120), "left_anti")
)

not_flagged = (
    kept_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([6, 25])), "left_anti")
)

result = (
    not_flagged
    .alias("s")
    .join(df_suppliers.alias("p"), F.col("s.supplier_id") == F.col("p.id"), "left")
    .groupBy(F.col("p.city").alias("city"), F.col("s.country").alias("product_country"))
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("s.cost")), 2).alias("total_cost"),
    )
)
