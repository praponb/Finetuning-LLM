from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("supplier_id").orderBy("tags_count", "id").rowsBetween(-2, Window.currentRow)
w2 = Window.partitionBy("brand").orderBy(F.col("cost").asc_nulls_last(), "id")
w_grp = Window.partitionBy("city").orderBy("last3_max_price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("tags_count", "id").rowsBetween(-5, -1)

filtered = (
    df_products
    .filter(F.col("brand").eqNullSafe(F.lit("Initech")))
    .withColumn("tags_count", F.size("tags"))
    .withColumn("tags_count_2", F.size("tags"))
)

enriched = (
    filtered
    .withColumn("last3_min_price", F.min("price").over(w))
    .withColumn("last3_avg_price", F.avg("price").over(w))
    .withColumn("last3_max_price", F.max("price").over(w))
)

eligible = (
    enriched
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.quantity") != 7), "left_anti")
)

with_dims = (
    eligible
    .alias("s")
    .join(df_suppliers.alias("b"), F.col("s.supplier_id") == F.col("b.id"), "left")
    .select("s.*", F.col("b.supplier_name"), F.col("b.country"), F.col("b.city"))
)

deduped = (
    with_dims
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
    .select(
        "id",
        "category",
        "brand",
        "price",
        "cost",
        "launch_date",
        "tags",
        "tags_count",
        "last3_max_price",
        "supplier_name",
        "country",
        "city",
    )
)

scored = (
    deduped
    .filter(F.array_contains("tags", "eco"))
    .select(
        "id",
        "category",
        "price",
        "tags_count",
        "last3_max_price",
        F.count("last3_max_price").over(w_grp).alias("running_count_last3_max_price"),
        F.max("last3_max_price").over(w_grp).alias("running_max_last3_max_price"),
        F.min("last3_max_price").over(w_grp).alias("running_min_last3_max_price"),
        F.avg("last3_max_price").over(w_grp).alias("running_avg_last3_max_price"),
        F.avg("last3_max_price").over(w4).alias("last3_max_price_avg_5_prior"),
    )
)

stats = (
    scored
    .rollup("category")
    .agg(
        F.count_if(F.col("category") == "Fashion").alias("fashion_events"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.count("*").alias("event_count"),
        F.count(F.when(F.col("category") == "Sports", 1)).alias("sports_count"),
        F.percentile_approx("last3_max_price", [0.25, 0.5, 0.75]).alias("last3_max_price_quartiles"),
        F.grouping("category").alias("grouping_level"),
    )
)

result = (
    stats
    .select("category", "fashion_events", "home_count", "event_count", "grouping_level")
    .orderBy(F.col("category").desc_nulls_last())
)
