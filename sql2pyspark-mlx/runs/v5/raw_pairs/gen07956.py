from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue_per_unit", "id").rowsBetween(-3, Window.currentRow)
w2 = Window.partitionBy("store_id").orderBy(F.col("last4_count_units").desc_nulls_last(), "id")
w_grp = Window.partitionBy("store_id").orderBy("last4_count_units", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("store_id").orderBy("dedup_rank", "id")
w6 = Window.partitionBy("product_id", "category").orderBy(F.col("last4_count_units").desc(), "id")
quantity_by_product_id = df_inventory.groupBy("product_id").agg(F.min("quantity").alias("min_inventory_quantity"))

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.to_date(F.lit("2023-03-08")))
    .withColumn("revenue_per_unit", F.round((F.col("units") / F.col("revenue")), 2))
)

scored = (
    cleaned
    .withColumn("last4_count_units", F.count("units").over(w))
    .withColumn("last4_sum_units", F.sum("units").over(w))
    .withColumn("last4_avg_units", F.avg("units").over(w))
)

ranked = (
    scored
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
)

metrics = (
    ranked
    .select(
        "id",
        "product_id",
        "units",
        "revenue",
        "revenue_per_unit",
        "last4_count_units",
        "dedup_rank",
        F.ntile(5).over(w_grp).alias("last4_count_units_bucket"),
        F.avg("last4_avg_units").over(w4).alias("product_id_avg_last4_avg_units"),
        F.lead("last4_count_units", 2).over(w5).alias("next_last4_count_units"),
    )
)

joined = (
    metrics
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.product_name"), F.col("b.price"), F.col("b.category"))
)

deduped = (
    joined
    .withColumn("row_num", F.row_number().over(w6))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "product_id",
        "dedup_rank",
        "last4_count_units_bucket",
        "next_last4_count_units",
        "price",
        "category",
    )
)

valid = (
    deduped
    .filter(F.col("category").isin(["Books", "Electronics"]))
    .select(
        "id",
        "product_id",
        "dedup_rank",
        "last4_count_units_bucket",
        "price",
        "category",
        F.least("dedup_rank", "last4_count_units_bucket").alias("least_dedup_rank_last4_count_units_bucket"),
        F.coalesce("category", F.lit("OTHER")).alias("category_filled"),
    )
)

result = (
    valid
    .join(quantity_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "dedup_rank",
        "last4_count_units_bucket",
        "price",
        "category",
        "least_dedup_rank_last4_count_units_bucket",
        "category_filled",
        F.col("min_inventory_quantity"),
    )
    .orderBy(F.col("min_inventory_quantity").desc())
)
