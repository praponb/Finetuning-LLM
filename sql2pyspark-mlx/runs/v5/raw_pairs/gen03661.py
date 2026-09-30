from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").asc_nulls_first(), "id")
w2 = Window.partitionBy("store_id", "product_id").orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("price", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

joined = (
    df_inventory
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date").between("2023-06-03", "2024-03-01"))
    .select("a.*", F.col("p.price"))
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
        "*",
        F.max("quantity").over(w2).alias("running_max_quantity"),
        F.min("quantity").over(w2).alias("running_min_quantity"),
        F.sum("quantity").over(w2).alias("running_sum_quantity"),
        F.last("quantity", ignorenulls=True).over(w3).alias("last_known_quantity"),
    )
)

enriched = (
    metrics
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.city"), F.col("b.sqft"), F.col("b.region"))
)

result = (
    enriched
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "price",
        "running_max_quantity",
        "city",
        "sqft",
        F.when(F.col("city").rlike("^(Chiang Mai|London)$"), "GROUP_B").otherwise("PRIORITY").alias("city_zone"),
        F.col("min_store_sale_units"),
    )
    .orderBy("price")
)
