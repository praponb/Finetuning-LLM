from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.last_restock").desc(), F.col("c.id").desc())
w_part = Window.partitionBy("city", "supplier_id").orderBy("quantity", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.partitionBy("supplier_id", "city")
w4 = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.sale_date").desc(), F.col("c.id").desc())
price_by_supplier_id = df_products.groupBy("supplier_id").agg(F.min("price").alias("min_product_price"))

enriched = (
    df_products
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .filter(F.col("d.city") == "Chiang Mai")
    .select(
        "a.id",
        "a.product_name",
        "a.cost",
        "a.launch_date",
        "a.supplier_id",
        F.col("d.rating"),
        F.col("d.country"),
        F.col("d.city"),
    )
)

latest_match = (
    enriched
    .alias("s")
    .join(df_inventory.alias("c"), (F.col("c.product_id") == F.col("s.id")) & (F.col("c.last_restock") <= F.col("s.launch_date")), "left")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.product_name",
        "s.supplier_id",
        "s.rating",
        "s.city",
        F.col("c.quantity"),
    )
)

metrics = (
    latest_match
    .select(
        "id",
        "product_name",
        "supplier_id",
        "rating",
        "city",
        F.min("quantity").over(w_part).alias("last3_min_quantity"),
        F.count("quantity").over(w_part).alias("last3_count_quantity"),
        F.avg("quantity").over(w_part).alias("last3_avg_quantity"),
        F.sum("quantity").over(w3).alias("supplier_id_sum_quantity"),
    )
)

with_last = (
    metrics
    .alias("s")
    .join(df_store_sales.alias("c"), (F.col("c.product_id") == F.col("s.id")), "left")
    .withColumn("rn", F.row_number().over(w4))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.supplier_id",
        "s.city",
        "s.last3_min_quantity",
        "s.supplier_id_sum_quantity",
        F.col("c.units").alias("store_sale_units"),
    )
)

result = (
    with_last
    .join(price_by_supplier_id, on="supplier_id", how="left")
    .select(
        "id",
        "supplier_id",
        "city",
        "last3_min_quantity",
        "supplier_id_sum_quantity",
        "store_sale_units",
        F.when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "EXTENDED").when(F.col("city").rlike("^(New York|Singapore)$"), "OTHER_ZONE").otherwise("GROUP_A").alias("city_zone"),
        F.col("min_product_price"),
    )
    .orderBy("supplier_id", F.col("min_product_price").desc())
)
