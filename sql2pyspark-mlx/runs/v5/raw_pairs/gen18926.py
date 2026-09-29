from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("brand").orderBy("cost", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_grp = Window.partitionBy("supplier_id").orderBy("price", "id")
w3 = Window.partitionBy("brand").orderBy("next_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("brand").orderBy("running_sum_next_price", "id")
w5 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())

with_windows = (
    df_products
    .select(
        "id",
        "brand",
        "cost",
        "stock_qty",
        "tags",
        F.count("price").over(w_part).alias("running_count_price"),
        F.max("price").over(w_part).alias("running_max_price"),
        F.min("price").over(w_part).alias("running_min_price"),
        F.lead("price", 1).over(w_grp).alias("next_price"),
    )
)

not_flagged = (
    with_windows
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.product_id").isin([4, 5])), "left_anti")
)

with_windows_products = (
    not_flagged
    .select(
        "id",
        "brand",
        "cost",
        "next_price",
        F.avg("next_price").over(w3).alias("running_avg_next_price"),
        F.sum("next_price").over(w3).alias("running_sum_next_price"),
        F.max("next_price").over(w3).alias("running_max_next_price"),
    )
)

ranked = (
    with_windows_products
    .withColumn("row_num", F.row_number().over(w4))
    .filter(F.col("row_num") == 1)
    .select(
        "id",
        "brand",
        "next_price",
        "running_sum_next_price",
        "running_max_next_price",
    )
)

with_latest = (
    ranked
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w5))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.revenue"), F.col("c.units").alias("store_sale_units"))
)

kept_keys = (
    with_latest
    .select("brand", "id")
    .exceptAll(
        df_products
        .filter(F.col("product_name").isNotNull())
        .select("brand", "id")
    )
)

reconciled = (
    with_latest
    .alias("s")
    .join(kept_keys.alias("k"), (F.col("s.brand") == F.col("k.brand")) & (F.col("s.id") == F.col("k.id")), "inner")
    .select("s.*")
)

result = (
    reconciled
    .withColumn("running_max_next_price_tier", F.when(F.col("running_max_next_price") >= 1020, "PREMIUM").otherwise("STANDARD"))
    .cube("brand", "running_max_next_price_tier")
    .agg(
        F.sum(F.when(F.col("brand") == "Globex", F.col("running_sum_next_price")).otherwise(0)).alias("globex_running_sum_next_price"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.round(F.sum("running_sum_next_price") / F.nullif(F.count(F.when(F.col("brand") == "Stark", 1)), F.lit(0)), 2).alias("running_sum_next_price_per_stark"),
        F.count("*").alias("row_count"),
        F.avg("store_sale_units").alias("avg_store_sale_units"),
        F.grouping("brand").alias("grouping_level"),
    )
)
