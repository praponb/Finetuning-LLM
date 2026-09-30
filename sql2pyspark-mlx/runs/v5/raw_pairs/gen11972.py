from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id")
w_part = Window.partitionBy("store_id")
w3 = Window.partitionBy("store_id", "category").orderBy(F.col("revenue").desc(), "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-24").cast("date")) & (F.col("revenue") < 3900))
    .select(
        "*",
        F.round((F.col("revenue") / F.col("units")), 2).alias("revenue_per_unit"),
    )
)

enriched = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .select("a.*", F.col("d.product_name"), F.col("d.price"))
)

scored = (
    enriched
    .withColumn("units_share", F.round((F.col("units") / F.sum("units").over(w)), 4))
    .withColumn("store_id_count_revenue", F.count("revenue").over(w_part))
)

enriched_store_sales = (
    scored
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.revenue",
        "src.revenue_per_unit",
        "src.product_name",
        "src.units_share",
        "src.store_id_count_revenue",
        F.col("p.cost"),
        F.col("p.category"),
        F.col("p.product_name").alias("product_product_name"),
    )
)

ranked = (
    enriched_store_sales
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy("store_id")
    .pivot("category", ["Grocery", "Books"])
    .agg(F.max("cost"))
)
