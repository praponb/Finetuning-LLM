from pyspark.sql import functions as F

kept_keys = (
    df_products
    .select("id")
    .exceptAll(
        df_products
        .filter(F.col("price") <= 730)
        .select("id")
    )
)

verified = (
    df_products
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    verified
    .cube(
        F.when(F.col("price") >= 1170, "PREMIUM").otherwise("STANDARD").alias("price_tier"),
    )
    .agg(
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.sum(F.when(F.col("brand") == "Acme", F.col("stock_qty")).otherwise(0)).alias("acme_stock_qty"),
        F.stddev_samp("cost").alias("cost_stddev"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
        F.sort_array(F.collect_set("category")).alias("category_list"),
    )
    .filter(F.col("home_events") > 1)
)
