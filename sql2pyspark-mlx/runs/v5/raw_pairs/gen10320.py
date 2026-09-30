from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city")
w2 = Window.partitionBy("city").orderBy("stock_qty", "id")
w3 = Window.partitionBy("supplier_id").orderBy("price_per_stock_qty", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("supplier_id").isNotNull() & (~F.col("brand").eqNullSafe(F.lit("Initech"))))
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        (F.col("price") / F.nullif("stock_qty", F.lit(0))).alias("price_per_stock_qty"),
        F.substring("brand", 1, 3).alias("brand_prefix"),
    )
)

cleaned = (
    filtered
    .filter(F.col("supplier_id") == 31)
    .withColumn("sorted_tags", F.sort_array("tags"))
    .withColumn("margin", F.round((F.col("cost") / F.col("price")), 2))
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country").isin(["UK", "DE"]))
    .select("src.*", F.col("s.city"), F.col("s.rating"))
)

metrics = (
    with_dims
    .select(
        "*",
        F.avg("rating").over(w).alias("city_avg_rating"),
        F.lead("price_per_stock_qty", 2, 0).over(w2).alias("next_price_per_stock_qty"),
        F.first("cost", ignorenulls=True).over(w3).alias("first_known_cost"),
    )
)

result = (
    metrics
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "tags",
        "price_per_stock_qty",
        "brand_prefix",
        "margin",
        "city",
        F.when(F.col("product_name").rlike("^(Running Shoes|  Water Bottle)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("product_name_zone"),
    )
    .orderBy(F.col("price").desc_nulls_first())
)
