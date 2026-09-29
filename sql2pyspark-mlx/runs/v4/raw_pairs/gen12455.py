from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.partitionBy("category", "brand").orderBy(F.col("stock_qty").desc_nulls_first())
w_recent = Window.partitionBy("category").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand", "category")
w5 = Window.partitionBy("brand").orderBy("first_known_cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("brand") == "Initech")
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        F.sort_array("tags").alias("sorted_tags"),
    )
)

enriched = (
    filtered
    .select(
        "*",
        F.first("cost", ignorenulls=True).over(w).alias("first_known_cost"),
        F.percent_rank().over(w2).alias("stock_qty_percent_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("brand").isNotNull())
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "first_known_cost",
        F.coalesce("brand", F.lit("N/A")).alias("brand_filled"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

result = (
    cleaned
    .withColumn("running_first_known_cost", F.sum("first_known_cost").over(w_recent))
    .withColumn("cumulative_stock_qty", F.sum("stock_qty").over(w_recent))
    .withColumn("brand_avg_cost", F.avg("cost").over(w4))
    .withColumn("first_known_stock_qty", F.first("stock_qty", ignorenulls=True).over(w5))
)
