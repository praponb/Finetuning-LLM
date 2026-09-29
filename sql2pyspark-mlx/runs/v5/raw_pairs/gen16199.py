from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("distinct_ids").desc())

cleaned = (
    df_inventory
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-07-15"))) & F.col("store_id").isin([1, 2, 3, 4]))
    .withColumn("quantity_band", F.when(F.col("quantity") >= 258, "High").when(F.col("quantity") >= 90, "Medium").otherwise("Low"))
)

with_dims = (
    cleaned
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.sqft"), F.col("b.city"), F.col("b.opened_date"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("b"), F.col("a.product_id") == F.col("b.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.quantity",
        "a.last_restock",
        "a.quantity_band",
        "a.opened_date",
        F.col("b.stock_qty"),
        F.col("b.price"),
        F.col("b.cost"),
    )
)

deduped = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

filtered = (
    deduped
    .filter((F.col("last_restock") > F.lit("2023-03-01").cast("date")) & (F.col("stock_qty") > 358))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "quantity_band",
        "opened_date",
        "stock_qty",
        "price",
        "cost",
        F.ceil("stock_qty").alias("stock_qty_whole"),
        F.weekofyear("opened_date").alias("opened_date_weekofyear"),
    )
)

result = (
    filtered
    .groupBy("product_id", F.date_trunc("WEEK", "last_restock").alias("week_start"))
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.percentile_approx("quantity", [0.25, 0.5, 0.75]).alias("quantity_quartiles"),
    )
    .withColumn("distinct_ids_percent_rank", F.percent_rank().over(w2))
    .orderBy(F.col("distinct_ids").desc())
)
