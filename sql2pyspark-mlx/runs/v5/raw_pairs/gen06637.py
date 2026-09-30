from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy("sale_date", "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy(F.col("units").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("units").desc(), "id")
w4 = Window.partitionBy("store_id")
w5 = Window.partitionBy("product_id")

ranked = (
    df_store_sales
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    ranked
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.brand"))
)

latest = (
    enriched
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked_store_sales = (
    latest
    .withColumn("dedup_rank", F.row_number().over(w3))
    .filter(F.col("dedup_rank") == 1)
)

enriched_store_sales = (
    ranked_store_sales
    .filter(F.col("revenue") >= 1500)
    .select(
        "*",
        F.sum("units").over(w4).alias("store_id_sum_units"),
        F.round((F.col("units") / F.sum("units").over(w5)), 4).alias("units_share"),
    )
)

result = (
    enriched_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .groupingSets(
        [["category", "product_brand"], ["category"], []],
        F.col("p.category").alias("category"),
        F.col("p.brand").alias("product_brand"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.col("a.revenue")).alias("total_revenue"),
        F.min(F.col("a.units_share")).alias("min_units_share"),
    )
    .orderBy(F.col("product_brand").desc())
)
