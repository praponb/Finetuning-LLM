from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("city").orderBy(F.col("price").desc(), "id")
w3 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.stock_qty").desc(), F.col("c.id").desc())
w_hist = Window.partitionBy("category").orderBy("is_fashion", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("city").orderBy(F.col("price_adjusted").asc_nulls_first())

filtered = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("rating").between(2.5, 3.3))
    .select(
        "id",
        "supplier_name",
        "city",
        F.trim("supplier_name").alias("clean_supplier_name"),
    )
)

latest_match = (
    filtered
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price"))
)

ranked = (
    latest_match
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest_match_suppliers = (
    ranked
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w3))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.price").alias("product_price"), F.col("c.category"))
)

filtered_suppliers = (
    latest_match_suppliers
    .filter(F.col("city").isNotNull() & (~F.col("category").eqNullSafe(F.lit("Sports"))))
    .withColumn("is_fashion", F.when(F.col("category") == "Fashion", 1).otherwise(0))
    .withColumn("price_adjusted", F.round((F.col("price") * 1.1), 2))
)

enriched = (
    filtered_suppliers
    .filter(F.col("price_adjusted").isNull())
    .select(
        "id",
        "supplier_name",
        "price",
        "product_price",
        "is_fashion",
        "price_adjusted",
        F.max("price").over(w_hist).alias("running_max_price"),
        F.count("price").over(w_hist).alias("running_count_price"),
        F.sum("price").over(w_hist).alias("running_sum_price"),
        F.percent_rank().over(w5).alias("price_adjusted_percent_rank"),
    )
)

stats = (
    enriched
    .withColumn("running_count_price_tier", F.when(F.col("running_count_price") >= 15, "LARGE").otherwise("SMALL"))
    .withColumn("price_adjusted_percent_rank_tier", F.when(F.col("price_adjusted_percent_rank") >= 0.69, "HIGH").otherwise("LOW"))
    .groupingSets(
        [["running_count_price_tier", "price_adjusted_percent_rank_tier"], ["running_count_price_tier"]],
        "running_count_price_tier",
        "price_adjusted_percent_rank_tier",
    )
    .agg(
        F.percentile_approx("running_max_price", 0.5).alias("running_max_price_p50"),
        F.round(F.avg("price_adjusted_percent_rank"), 2).alias("avg_price_adjusted_percent_rank"),
        F.count("*").alias("num_rows"),
    )
    .filter(F.col("avg_price_adjusted_percent_rank") > 0)
)

result = (
    stats
    .select("running_count_price_tier", "price_adjusted_percent_rank_tier", "num_rows")
)
