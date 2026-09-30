from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id")
w2 = Window.partitionBy("category", "supplier_id")
w_running = Window.partitionBy("category", "supplier_id").orderBy(F.col("stock_qty").asc_nulls_last())
w4 = Window.partitionBy("brand").orderBy(F.col("greatest_cost_stock_qty").desc_nulls_first())
w_recent = Window.orderBy(F.col("cost").asc_nulls_first())
w6 = Window.partitionBy("category")
w7 = Window.partitionBy("supplier_id").orderBy(F.col("corr_cost_stock_qty").desc_nulls_first())
w8 = Window.partitionBy("brand")
w9 = Window.partitionBy("supplier_id").orderBy(F.col("cost_rank_share").desc_nulls_first())
w10 = Window.orderBy(F.col("cost_rank_share").asc_nulls_first())
w11 = Window.orderBy(F.col("corr_cost_stock_qty").desc_nulls_last())

base = (
    df_products
    .filter(F.col("launch_date") > F.to_date(F.lit("2023-04-09")))
    .select(
        "*",
        F.greatest("cost", "stock_qty").alias("greatest_cost_stock_qty"),
        F.coalesce("brand", F.lit("NONE")).alias("resolved_brand"),
    )
)

kept_keys = (
    base
    .select("id", "supplier_id")
    .subtract(
        df_products
        .filter(F.col("stock_qty") >= 219)
        .select("id", "supplier_id")
    )
)

verified = (
    base
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.supplier_id") == F.col("k.supplier_id")), "inner")
    .select("s.*")
)

enriched = (
    verified
    .withColumn("supplier_id_min_stock_qty", F.min("stock_qty").over(w))
    .withColumn("corr_cost_stock_qty", F.corr("cost", "stock_qty").over(w2))
    .withColumn("stock_qty_rank", F.rank().over(w_running))
    .withColumn("greatest_cost_stock_qty_percent_rank", F.percent_rank().over(w4))
    .withColumn("cost_rank", F.rank().over(w_recent))
)

metrics = (
    enriched
    .filter(F.array_contains("tags", "sale"))
    .select(
        "product_name",
        "category",
        "brand",
        "price",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "resolved_brand",
        "supplier_id_min_stock_qty",
        "corr_cost_stock_qty",
        "cost_rank",
        F.round((F.col("cost_rank") / F.sum("cost_rank").over(w6)), 4).alias("cost_rank_share"),
    )
)

enriched_products = (
    metrics
    .select(
        "product_name",
        "category",
        "stock_qty",
        "launch_date",
        "supplier_id",
        "resolved_brand",
        "cost_rank_share",
        F.cume_dist().over(w7).alias("corr_cost_stock_qty_cume_dist"),
        F.avg("cost_rank").over(w8).alias("brand_avg_cost_rank"),
        F.dense_rank().over(w9).alias("cost_rank_share_dense_rank"),
        F.percent_rank().over(w10).alias("cost_rank_share_percent_rank"),
        F.dense_rank().over(w11).alias("corr_cost_stock_qty_dense_rank"),
    )
)

base_products = (
    enriched_products
    .filter((F.col("launch_date") >= F.lit("2023-08-22").cast("date")) & F.col("supplier_id").isNotNull())
    .select(
        "*",
        (F.col("corr_cost_stock_qty_cume_dist") - F.col("cost_rank_share_percent_rank")).alias("corr_cost_stock_qty_cume_dist_minus_cost_rank_share_percent_rank"),
        F.concat("product_name", F.lit(" "), "category").alias("product_name_category"),
    )
)

with_dims = (
    base_products
    .alias("a")
    .join(df_suppliers.alias("p"), F.col("a.supplier_id") == F.col("p.id"), "left")
    .select(
        "a.product_name",
        "a.category",
        "a.stock_qty",
        "a.supplier_id",
        "a.resolved_brand",
        "a.brand_avg_cost_rank",
        "a.cost_rank_share_dense_rank",
        F.col("p.country"),
    )
)

result = (
    with_dims
    .groupBy("category")
    .pivot("country", ["DE", "US"])
    .agg(
        F.max("cost_rank_share_dense_rank").alias("amount"),
        F.count(F.lit(1)).alias("cnt"),
    )
)
