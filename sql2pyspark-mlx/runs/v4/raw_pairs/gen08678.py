from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "brand").orderBy(F.col("revenue_rounded").asc_nulls_last(), "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("revenue_rounded").desc(), "id")

valid = (
    df_store_sales
    .filter(F.col("product_id").isin([5, 8]))
    .select("*", F.round("revenue", 0).alias("revenue_rounded"))
)

joined = (
    valid
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.brand"), F.col("p.category"))
)

latest = (
    joined
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

latest_store_sales = (
    latest
    .withColumn("dedup_rank_2", F.row_number().over(w2))
    .filter(F.col("dedup_rank_2") == 1)
    .drop("dedup_rank_2")
)

base = (
    latest_store_sales
    .filter((F.col("sale_date") > F.lit("2024-03-02").cast("date")) & F.col("brand").isNotNull() & F.col("units").isNotNull())
    .select(
        "*",
        F.concat_ws("-", "category", "brand").alias("category_brand_key"),
        F.round((F.col("dedup_rank") * 1.07), 2).alias("dedup_rank_scaled"),
    )
)

result = (
    base
    .select(
        "id",
        "sale_date",
        "units",
        "revenue",
        "revenue_rounded",
        "brand",
        "dedup_rank",
        "category_brand_key",
        "dedup_rank_scaled",
    )
)
