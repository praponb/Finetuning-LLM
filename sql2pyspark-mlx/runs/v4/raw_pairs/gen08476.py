from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("brand").orderBy(F.col("launch_date").desc_nulls_first())
w2 = Window.partitionBy("brand").orderBy(F.col("price").desc_nulls_last())
w3 = Window.partitionBy("category", "supplier_id")

latest = (
    df_products
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

remaining_keys = (
    latest
    .select("category")
    .subtract(
        df_products
        .filter(F.col("cost").between(370, 540))
        .select("category")
    )
)

reconciled = (
    latest
    .alias("s")
    .join(remaining_keys.alias("k"), (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

metrics = (
    reconciled
    .select(
        "*",
        F.dense_rank().over(w2).alias("price_dense_rank"),
        F.round((F.col("cost") / F.sum("cost").over(w3)), 4).alias("cost_share"),
    )
)

remaining_keys_2 = (
    metrics
    .select("id", "category")
    .subtract(
        df_products
        .filter(F.col("brand") != "Globex")
        .select("id", "category")
    )
)

verified = (
    metrics
    .alias("s")
    .join(remaining_keys_2.alias("k"), (F.col("s.id") == F.col("k.id")) & (F.col("s.category") == F.col("k.category")), "inner")
    .select("s.*")
)

totals = (
    verified
    .groupBy(
        F.when(F.col("stock_qty") >= 392, "PREMIUM").otherwise("STANDARD").alias("stock_qty_tier"),
    )
    .agg(
        F.percentile_approx("cost", [0.25, 0.5, 0.75]).alias("cost_quartiles"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.round(F.sum("cost") / F.nullif(F.count(F.when(F.col("brand") == "Acme", 1)), F.lit(0)), 2).alias("cost_per_acme"),
        F.stddev_samp("price").alias("price_stddev"),
    )
)

result = (
    totals
    .select(
        "stock_qty_tier",
        "cost_quartiles",
        "category_list",
        "cost_per_acme",
        "price_stddev",
    )
    .orderBy(F.col("price_stddev").asc_nulls_first(), F.col("cost_per_acme").desc())
)
