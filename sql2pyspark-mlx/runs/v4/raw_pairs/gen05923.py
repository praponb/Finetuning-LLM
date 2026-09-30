from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy("unit_price", "id")
w2 = Window.partitionBy("product_id", "brand").orderBy("unit_price", "id")
w3 = Window.orderBy("unit_price", "id").rowsBetween(-5, -1)
w4 = Window.partitionBy("brand").orderBy(F.col("quantity_whole").desc(), "id")
w5 = Window.partitionBy("brand", "order_id").orderBy("cost_adjusted", "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & (F.col("unit_price") < 150))
    .select("*", F.ceil("quantity").alias("quantity_whole"))
)

joined = (
    valid
    .alias("e")
    .join(df_products.alias("p"), F.col("e.product_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.brand"), F.col("p.product_name"), F.col("p.cost"))
)

filtered = (
    joined
    .filter(F.col("brand") == "Initech")
    .select(
        "*",
        F.round((F.col("quantity_whole") / F.col("cost")), 2).alias("quantity_whole_per_cost"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean"),
    )
)

scored = (
    filtered
    .withColumn("unit_price_bucket", F.ntile(3).over(w))
    .withColumn("unit_price_bucket_2", F.ntile(5).over(w2))
    .withColumn("cost_avg_5_prior", F.avg("cost").over(w3))
)

valid_order_items = (
    scored
    .filter(F.col("product_id").isin([18, 37]))
    .select(
        "id",
        "order_id",
        "quantity",
        "unit_price",
        "quantity_whole",
        "brand",
        "product_name",
        "quantity_whole_per_cost",
        "brand_clean",
        "unit_price_bucket",
        F.round((F.col("cost") * 100), 2).alias("cost_adjusted"),
        F.least("cost_avg_5_prior", "quantity").alias("least_cost_avg_5_prior_quantity"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_clean_2"),
    )
)

with_windows = (
    valid_order_items
    .withColumn("quantity_whole_bucket", F.ntile(10).over(w4))
    .withColumn("prev_quantity", F.lag("quantity", 1).over(w5))
)

result = (
    with_windows
    .groupBy("brand")
    .agg(
        F.sum(F.when(F.col("brand") == "Stark", F.col("least_cost_avg_5_prior_quantity"))).alias("stark_total_least_cost_avg_5_prior_quantity"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("quantity_whole", 0.9).alias("quantity_whole_p90"),
    )
)
