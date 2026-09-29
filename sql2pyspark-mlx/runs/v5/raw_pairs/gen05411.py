from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category").orderBy(F.col("quantity").asc_nulls_last(), "id")
w2 = Window.partitionBy("brand")
w3 = Window.orderBy("unit_price", "id")
w4 = Window.partitionBy("category", "brand").orderBy(F.col("unit_price").desc(), "id")
w5 = Window.partitionBy("order_id").orderBy("quantity", "id")

filtered = (
    df_order_items
    .filter(F.col("product_id").isNotNull())
    .withColumn("unit_price_whole", F.floor("unit_price"))
)

joined = (
    filtered
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "left")
    .filter(F.col("d.stock_qty") < 183)
    .select(
        "a.id",
        "a.order_id",
        "a.product_id",
        "a.quantity",
        "a.unit_price",
        "a.unit_price_whole",
        F.col("d.brand"),
        F.col("d.category"),
    )
)

latest = (
    joined
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

metrics = (
    latest
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        "brand",
        F.sum("unit_price").over(w2).alias("brand_sum_unit_price"),
        F.lead("unit_price", 1).over(w3).alias("next_unit_price"),
        F.ntile(4).over(w4).alias("unit_price_bucket"),
    )
)

deduped = (
    metrics
    .withColumn("row_num", F.row_number().over(w5))
    .filter(F.col("row_num") == 1)
)

cleaned = (
    deduped
    .filter(F.col("brand").isNotNull())
    .select(
        "*",
        F.round((F.col("row_num") / F.col("unit_price_bucket")), 2).alias("row_num_per_unit_price_bucket"),
        (F.col("brand_sum_unit_price") * F.col("unit_price_bucket")).alias("brand_sum_unit_price_times_unit_price_bucket"),
    )
)

enriched = (
    cleaned
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.price"), F.col("p.launch_date"))
)

result = (
    enriched
    .rollup(
        "product_id",
        F.when(F.col("row_num_per_unit_price_bucket") >= 77, "HIGH").otherwise("LOW").alias("row_num_per_unit_price_bucket_tier"),
    )
    .agg(
        F.count_if(F.col("brand") == "Umbrella").alias("umbrella_events"),
        F.size(F.collect_set("brand")).alias("num_brands"),
        F.percentile_approx("unit_price_bucket", [0.25, 0.5, 0.75]).alias("unit_price_bucket_quartiles"),
        F.sort_array(F.collect_set("brand")).alias("brand_list"),
        F.size(F.collect_set("brand")).alias("num_brands_2"),
    )
    .orderBy(F.col("product_id").desc())
)
