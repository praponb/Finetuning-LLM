from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("cost").desc(), "id")

deduped = (
    df_products
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

not_flagged = (
    deduped
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.unit_price").between(130, 300)), "left_anti")
)

cleaned = (
    not_flagged
    .filter((F.col("launch_date") > F.lit("2024-03-12").cast("date")) & F.col("supplier_id").isNotNull() & F.col("category").isin(["Home", "Sports"]))
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
        "rank_in_group",
        (F.col("rank_in_group") + F.col("cost")).alias("rank_in_group_plus_cost"),
    )
)

summary = (
    cleaned
    .groupBy(
        F.when(F.col("price") >= 1260, "LARGE").otherwise("SMALL").alias("price_tier"),
        "category",
    )
    .agg(
        F.count("*").alias("event_count"),
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("event_count_2"),
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("price"))).alias("acme_total_price"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
)

result = (
    summary
    .select(
        "price_tier",
        "category",
        "event_count",
        "cost_quartiles",
        "acme_total_price",
    )
)
