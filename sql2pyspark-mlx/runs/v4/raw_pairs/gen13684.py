from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.units").desc(), F.col("c.id").desc())
cost_by_supplier_id = df_products.groupBy("supplier_id").agg(F.sum("cost").alias("sum_product_cost"))

filtered = (
    df_products
    .filter(F.col("cost") >= 630)
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "stock_qty",
        "launch_date",
        "tags",
        "supplier_id",
        F.array_contains("tags", "premium").alias("is_premium"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

eligible = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") <= F.col("s.launch_date")), "left_semi")
)

filtered_products = (
    eligible
    .filter((F.col("launch_date") > F.to_date(F.lit("2023-10-30"))) & F.col("supplier_id").isNotNull())
    .withColumn("cost_adjusted", F.round((F.col("cost") * 100), 2))
)

with_latest = (
    filtered_products
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.units"))
)

filtered_products_2 = (
    with_latest
    .filter(F.col("launch_date") > F.lit("2024-01-07").cast("date"))
    .select("*", F.upper("category").alias("category_upper"))
)

joined = (
    filtered_products_2
    .alias("e")
    .join(df_suppliers.alias("b"), F.col("e.supplier_id") == F.col("b.id"), "left")
    .select("e.*", F.col("b.supplier_name"))
)

wide = (
    joined
    .groupBy("supplier_id")
    .pivot("category", ["Sports", "Home", "Books"])
    .agg(F.sum("cost_adjusted"))
)

result = (
    wide
    .join(cost_by_supplier_id, on="supplier_id", how="left")
    .select("supplier_id", "Sports", "Home", F.col("sum_product_cost"))
)
