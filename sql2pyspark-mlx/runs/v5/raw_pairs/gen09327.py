from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id").orderBy(F.col("stock_qty").asc_nulls_first(), "id")

filtered = (
    df_products
    .filter(F.col("launch_date") > F.lit("2023-06-21").cast("date"))
    .select("*", F.weekofyear("launch_date").alias("launch_date_weekofyear"))
)

not_flagged = (
    filtered
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isNull()), "left_anti")
)

with_latest = (
    not_flagged
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.sale_date") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.sale_date").alias("store_sale_sale_date"))
)

deduped = (
    with_latest
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    deduped
    .alias("src")
    .join(df_suppliers.alias("p"), F.col("src.supplier_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.category",
        "src.price",
        "src.stock_qty",
        "src.tags",
        "src.supplier_id",
        "src.store_sale_sale_date",
        "src.dedup_rank",
        F.col("p.country"),
        F.col("p.rating"),
        F.col("p.city"),
    )
)

kept = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") < F.col("s.store_sale_sale_date")), "left_semi")
)

result = (
    kept
    .groupBy("country")
    .pivot("city", ["Singapore", "Bangkok"])
    .agg(F.max("price"))
    .select(
        "country",
        F.col("Singapore").alias("singapore"),
        F.col("Bangkok").alias("bangkok"),
    )
)
