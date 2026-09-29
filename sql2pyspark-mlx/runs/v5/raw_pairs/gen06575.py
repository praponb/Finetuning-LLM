from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.cost").desc(), F.col("c.id").desc())

base = (
    df_suppliers
    .filter(F.col("city").isNotNull() & F.col("city").isin(["New York", "Chiang Mai", "Singapore"]))
    .select(
        "id",
        "supplier_name",
        "city",
        F.regexp_replace("supplier_name", " ", "_").alias("supplier_name_slug"),
    )
)

filtered = (
    base
    .filter(F.col("city").isNotNull())
    .select(
        "*",
        F.when(F.col("city") == "Tokyo", 1).otherwise(0).alias("is_tokyo"),
        F.coalesce("city", F.lit("N/A")).alias("city_clean"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.price") >= 1180), "left_semi")
)

not_flagged = (
    eligible
    .alias("s")
    .join(df_products.alias("x"), (F.col("x.supplier_id") == F.col("s.id")) & (F.col("x.brand") == "Stark"), "left_anti")
)

latest_match = (
    not_flagged
    .alias("s")
    .join(df_products.alias("c"), (F.col("c.supplier_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.stock_qty").alias("product_stock_qty"))
)

base_suppliers = (
    latest_match
    .filter(F.col("city").isNotNull())
    .select(
        "id",
        "supplier_name",
        "city",
        "supplier_name_slug",
        "product_stock_qty",
        F.coalesce("supplier_name", F.lit("Unknown")).alias("supplier_name_filled"),
        F.length("supplier_name").alias("supplier_name_length"),
        F.coalesce("city", F.lit("DIRECT")).alias("city_filled"),
    )
)

result = (
    base_suppliers
    .groupBy("city")
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.count("*").alias("num_rows"),
        F.sum(F.when(F.col("city") == "London", F.col("product_stock_qty")).otherwise(0)).alias("london_product_stock_qty"),
        F.round(F.sum("supplier_name_length") / F.nullif(F.count(F.when(F.col("city") == "Chiang Mai", 1)), F.lit(0)), 2).alias("supplier_name_length_per_chiang_mai"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
    )
)
