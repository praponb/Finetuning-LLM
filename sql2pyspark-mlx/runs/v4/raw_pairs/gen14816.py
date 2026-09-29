from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first())
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc())
w3 = Window.partitionBy("store_id", "product_id").orderBy("quantity", "id").rowsBetween(-2, -1)
w4 = Window.partitionBy("week_start").orderBy(F.col("chiang_mai_total_cost").desc())

metrics = (
    df_inventory
    .select(
        "*",
        F.rank().over(w).alias("quantity_rank"),
        F.dense_rank().over(w2).alias("quantity_dense_rank"),
        F.avg("quantity").over(w3).alias("quantity_avg_2_prior"),
    )
)

with_dims = (
    metrics
    .alias("e")
    .join(df_stores.alias("p"), F.col("e.store_id") == F.col("p.id"), "inner")
    .select("e.*", F.col("p.city"), F.col("p.sqft"))
)

joined = (
    with_dims
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "left")
    .select(
        "e.*",
        F.col("b.opened_date"),
        F.col("b.city").alias("store_city"),
        F.col("b.region"),
    )
)

with_dims_inventory = (
    joined
    .alias("e")
    .join(df_products.alias("b"), F.col("e.product_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.cost"), F.col("b.category"), F.col("b.stock_qty"))
)

result = (
    with_dims_inventory
    .groupBy(
        F.date_trunc("week", "opened_date").alias("week_start"),
        F.when(F.col("quantity_avg_2_prior") >= 191, "PREMIUM").otherwise("STANDARD").alias("quantity_avg_2_prior_tier"),
        F.date_trunc("year", "opened_date").alias("year_start"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("store_city") == "Chiang Mai", F.col("cost"))).alias("chiang_mai_total_cost"),
        F.sort_array(F.collect_set("region")).alias("region_list"),
        F.bit_or("quantity").alias("quantity_bit_or"),
    )
    .withColumn("chiang_mai_total_cost_rank", F.rank().over(w4))
    .orderBy(F.col("num_rows").desc_nulls_first())
)
