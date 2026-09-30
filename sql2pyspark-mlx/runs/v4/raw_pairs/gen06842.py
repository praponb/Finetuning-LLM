from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("launch_date").desc_nulls_last(), "id")

ranked = (
    df_products
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

result = (
    ranked
    .groupBy(
        F.when(F.col("cost") >= 720, "PREMIUM").otherwise("STANDARD").alias("cost_tier"),
    )
    .agg(
        F.sum(F.when(F.col("category") == "Books", F.col("stock_qty")).otherwise(0)).alias("books_stock_qty"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.min("cost").alias("min_cost"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.countDistinct("id").alias("distinct_ids"),
        F.size(F.collect_set("brand")).alias("num_brands"),
    )
    .orderBy(F.col("min_cost").desc_nulls_last())
)
