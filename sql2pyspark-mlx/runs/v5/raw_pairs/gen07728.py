from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id").orderBy("revenue", "id")
quantity_by_product_id = df_order_items.groupBy("product_id").agg(F.avg("quantity").alias("avg_order_item_quantity"))

joined = (
    df_store_sales
    .alias("e")
    .join(df_stores.alias("d"), F.col("e.store_id") == F.col("d.id"), "left")
    .select("e.*", F.col("d.opened_date"), F.col("d.store_name"))
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
)

base = (
    latest
    .filter(F.col("sale_date") > F.lit("2023-04-18").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "opened_date",
        "store_name",
        "rank_in_group",
        F.coalesce("revenue", F.lit(0)).alias("revenue_filled"),
    )
)

ranked = (
    base
    .withColumn("latest_rank", F.row_number().over(w2))
    .filter(F.col("latest_rank") == 1)
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue",
        "rank_in_group",
        "revenue_filled",
    )
)

latest_store_sales = (
    ranked
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

totals = (
    latest_store_sales
    .withColumn("revenue_tier", F.when(F.col("revenue") >= 3400, "LARGE").otherwise("SMALL"))
    .cube("revenue_tier", "product_id")
    .agg(
        F.count("*").alias("row_count"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.min("revenue_filled").alias("min_revenue_filled"),
        F.count("*").alias("cnt"),
        F.avg("revenue").alias("avg_revenue"),
        F.grouping("revenue_tier").alias("grouping_level"),
    )
)

result = (
    totals
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "distinct_product_ids",
        "min_revenue_filled",
        "avg_revenue",
        "grouping_level",
        F.col("avg_order_item_quantity"),
    )
)
