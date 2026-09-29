from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price_scaled", "id")
w2 = Window.partitionBy("category").orderBy("revenue_per_unit_2", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("category", "product_id").orderBy(F.col("price_scaled").desc())

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-05-08").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        (F.col("revenue") * F.col("units")).alias("revenue_per_unit"),
        (F.col("units") + F.col("revenue")).alias("revenue_per_unit_2"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.brand"), F.col("b.category"), F.col("b.price"))
)

base = (
    enriched
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-09-05")))
    .withColumn("brand_filled", F.coalesce("brand", F.lit("Unknown")))
    .withColumn("revenue_per_unit_rounded", F.round("revenue_per_unit", 2))
)

valid = (
    base
    .filter((F.col("sale_date") >= F.lit("2023-02-18").cast("date")) & F.col("category").isNotNull())
    .select(
        "id",
        "product_id",
        "sale_date",
        "revenue_per_unit_2",
        "brand",
        "category",
        "brand_filled",
        "revenue_per_unit_rounded",
        F.floor("revenue_per_unit_rounded").alias("revenue_per_unit_rounded_whole"),
        F.round((F.col("price") * 1.07), 2).alias("price_scaled"),
        F.coalesce("category", F.lit("OTHER")).alias("category_filled"),
    )
)

enriched_store_sales = (
    valid
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.product_id",
        "src.revenue_per_unit_2",
        "src.category",
        "src.brand_filled",
        "src.price_scaled",
        "src.category_filled",
        F.col("p.cost"),
    )
)

ranked = (
    enriched_store_sales
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    ranked
    .filter(F.col("revenue_per_unit_2") >= 42)
    .select(
        "id",
        "product_id",
        "revenue_per_unit_2",
        "category",
        "brand_filled",
        "category_filled",
        F.count("revenue_per_unit_2").over(w2).alias("last3_count_revenue_per_unit_2"),
        F.sum("revenue_per_unit_2").over(w2).alias("last3_sum_revenue_per_unit_2"),
        F.max("revenue_per_unit_2").over(w2).alias("last3_max_revenue_per_unit_2"),
        F.cume_dist().over(w3).alias("price_scaled_cume_dist"),
    )
)

result = (
    metrics
    .select(
        "id",
        "revenue_per_unit_2",
        "category",
        "category_filled",
        "last3_count_revenue_per_unit_2",
        "last3_sum_revenue_per_unit_2",
        F.when(F.col("category").rlike("^(Grocery|Electronics)$"), "GROUP_B").when(F.col("category").rlike("^(Home|Sports)$"), "OTHER_ZONE").otherwise("CORE").alias("category_zone"),
    )
    .orderBy(F.col("last3_sum_revenue_per_unit_2").desc())
)
