from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("rating").desc(), "id")
w2 = Window.partitionBy("city").orderBy("price_plus_cost_int", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("city").orderBy("price_plus_cost_int", "id").rowsBetween(-2, Window.currentRow)
w4 = Window.partitionBy("city").orderBy("price_plus_cost_int", "id")

base = (
    df_products
    .filter(F.col("brand").isNotNull())
    .withColumn("stock_qty_rounded", F.round("stock_qty", 1))
    .withColumn("stock_qty_plus_price", (F.col("stock_qty") + F.col("price")))
)

enriched = (
    base
    .alias("a")
    .join(df_suppliers.alias("d"), F.col("a.supplier_id") == F.col("d.id"), "inner")
    .filter(F.col("d.country") == "US")
    .select(
        "a.id",
        "a.product_name",
        "a.price",
        "a.cost",
        "a.launch_date",
        "a.supplier_id",
        "a.stock_qty_plus_price",
        F.col("d.city"),
        F.col("d.rating"),
        F.col("d.supplier_name"),
    )
)

joined = (
    enriched
    .alias("a")
    .join(df_suppliers.alias("b"), F.col("a.supplier_id") == F.col("b.id"), "left")
    .select(
        "a.*",
        F.col("b.supplier_name").alias("supplier_supplier_name"),
        F.col("b.rating").alias("supplier_rating"),
    )
)

filtered = (
    joined
    .filter(F.col("city").isNotNull() & (F.col("price") >= 360))
    .select(
        "id",
        "product_name",
        "price",
        "cost",
        "launch_date",
        "stock_qty_plus_price",
        "city",
        "rating",
        "supplier_supplier_name",
        F.trim("supplier_name").alias("clean_supplier_name"),
        F.col("cost").cast("int").alias("cost_int"),
    )
)

base_products = (
    filtered
    .filter(F.col("city").isNotNull() & (F.col("cost_int") < 36))
    .select(
        "id",
        "product_name",
        "cost",
        "launch_date",
        "city",
        "rating",
        "supplier_supplier_name",
        (F.col("price") + F.col("cost_int")).alias("price_plus_cost_int"),
    )
)

deduped = (
    base_products
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "city", "supplier_supplier_name", "price_plus_cost_int")
)

result = (
    deduped
    .select(
        "id",
        "supplier_supplier_name",
        "price_plus_cost_int",
        F.sum("price_plus_cost_int").over(w2).alias("cumulative_price_plus_cost_int"),
        F.avg("price_plus_cost_int").over(w3).alias("price_plus_cost_int_avg_2"),
        F.ntile(4).over(w4).alias("price_plus_cost_int_bucket"),
    )
)
