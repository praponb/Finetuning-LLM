from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())

cleaned = (
    df_products
    .filter(F.col("price").between(380, 710))
    .select(
        "*",
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.concat("product_name", F.lit(" "), "category").alias("product_name_category"),
        F.coalesce("brand", F.lit("DIRECT")).alias("brand_clean"),
    )
)

cleaned_products = (
    cleaned
    .filter((F.col("launch_date") >= F.lit("2023-05-30").cast("date")) & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        "days_since_launch_date",
        "product_name_category",
        "brand_clean",
        F.round("price", 1).alias("price_rounded"),
    )
)

joined = (
    cleaned_products
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.country") == "US")
    .select("src.*", F.col("s.supplier_name"), F.col("s.country"), F.col("s.rating"))
)

with_latest = (
    joined
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.cost",
        "s.launch_date",
        "s.brand_clean",
        "s.supplier_name",
        "s.country",
        F.col("c.quantity").alias("order_item_quantity"),
        F.col("c.unit_price"),
    )
)

result = (
    with_latest
    .groupBy("category", "country")
    .agg(
        F.max("order_item_quantity").alias("max_order_item_quantity"),
        F.avg(F.when(F.col("brand") == "Stark", F.col("cost"))).alias("stark_avg_cost"),
    )
    .orderBy(F.col("country").asc_nulls_last())
)
