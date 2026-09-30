from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
w2 = Window.orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("supplier_id", "category").orderBy("revenue", "id")
w4 = Window.partitionBy("supplier_id").orderBy(F.col("price").desc(), "id")
w5 = Window.partitionBy("supplier_id", "category").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_products
    .filter(F.col("product_name").like("%Offi%"))
    .select(
        "*",
        F.substring("brand", 1, 4).alias("brand_prefix"),
        F.datediff(F.current_date(), "launch_date").alias("days_since_launch_date"),
        F.coalesce("brand", F.lit("OTHER")).alias("brand_clean"),
    )
)

with_last = (
    filtered
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.category",
        "s.brand",
        "s.price",
        "s.cost",
        "s.tags",
        "s.supplier_id",
        "s.brand_clean",
        F.col("c.sale_date"),
        F.col("c.revenue"),
    )
)

kept = (
    with_last
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.order_id").isin([9, 17])), "left_semi")
)

eligible = (
    kept
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.revenue") >= 3700), "left_semi")
)

enriched = (
    eligible
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "tags",
        "supplier_id",
        "revenue",
        F.sum("price").over(w2).alias("running_price"),
        F.lead("cost", 1).over(w3).alias("next_cost"),
        F.ntile(3).over(w4).alias("price_bucket"),
        F.last("revenue", ignorenulls=True).over(w5).alias("last_known_revenue"),
    )
)

with_dims = (
    enriched
    .alias("e")
    .join(df_suppliers.alias("s"), F.col("e.supplier_id") == F.col("s.id"), "left")
    .filter(F.col("s.rating") <= 3.3)
    .select("e.*", F.col("s.city"), F.col("s.supplier_name"))
)

result = (
    with_dims
    .alias("src")
    .join(df_suppliers.alias("s"), F.col("src.supplier_id") == F.col("s.id"), "left")
    .groupingSets(
        [["country", "category"], ["country"], []],
        F.col("s.country").alias("country"),
        F.col("src.category").alias("category"),
    )
    .agg(
        F.count("*").alias("cnt"),
        F.round(F.sum(F.col("src.next_cost")), 2).alias("total_next_cost"),
        F.grouping_id().alias("gid"),
    )
    .orderBy(F.col("total_next_cost").asc_nulls_first())
)
