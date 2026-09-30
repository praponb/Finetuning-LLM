from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.unit_price").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("supplier_id")
w3 = Window.partitionBy("supplier_id").orderBy("margin", "id")
w4 = Window.orderBy("margin", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_products
    .filter(F.col("brand").isNotNull() & (F.col("launch_date") > F.lit("2023-05-02").cast("date")))
    .select(
        "id",
        "product_name",
        "category",
        "price",
        "cost",
        "launch_date",
        "supplier_id",
        (F.col("price") * F.col("cost")).alias("margin"),
        F.array_contains("tags", "clearance").alias("is_clearance"),
    )
)

with_last = (
    base
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select(
        "s.id",
        "s.category",
        "s.price",
        "s.cost",
        "s.supplier_id",
        "s.margin",
        "s.is_clearance",
        F.col("c.unit_price"),
        F.col("c.quantity"),
    )
)

metrics = (
    with_last
    .filter(F.col("is_clearance"))
    .select(
        "id",
        "category",
        "price",
        "cost",
        "supplier_id",
        "margin",
        "quantity",
        F.max("quantity").over(w2).alias("supplier_id_max_quantity"),
        F.lag("unit_price", 1).over(w3).alias("prev_unit_price"),
        F.sum("price").over(w4).alias("running_price"),
    )
)

base_products = (
    metrics
    .filter(F.col("supplier_id").isNotNull())
    .select(
        "id",
        "category",
        "margin",
        "supplier_id_max_quantity",
        "prev_unit_price",
        (F.col("prev_unit_price") * F.col("supplier_id_max_quantity")).alias("prev_unit_price_times_supplier_id_max_quantity"),
        F.col("prev_unit_price").cast("int").alias("prev_unit_price_int"),
    )
)

kept = (
    base_products
    .alias("s")
    .join(df_order_items.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.order_id").isin([7, 11, 12, 26])), "left_anti")
)

result = (
    kept
    .cube(
        F.when(F.col("supplier_id_max_quantity") >= 8, "PREMIUM").otherwise("STANDARD").alias("supplier_id_max_quantity_tier"),
        F.when(F.col("prev_unit_price_int") >= 61, "HIGH").otherwise("LOW").alias("prev_unit_price_int_tier"),
    )
    .agg(
        F.round(F.sum("prev_unit_price_int") / F.nullif(F.count(F.when(F.col("category") == "Home", 1)), F.lit(0)), 2).alias("prev_unit_price_int_per_home"),
        F.countDistinct("id").alias("distinct_ids"),
        F.max("supplier_id_max_quantity").alias("max_supplier_id_max_quantity"),
        F.round(F.sum("margin") / F.nullif(F.count(F.when(F.col("category") == "Fashion", 1)), F.lit(0)), 2).alias("margin_per_fashion"),
    )
    .orderBy(F.col("supplier_id_max_quantity_tier").desc())
)
