from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy(F.col("s.id")).orderBy(F.col("c.quantity").desc(), F.col("c.id").desc())
w2 = Window.partitionBy("brand").orderBy("price", "id")
w3 = Window.partitionBy("supplier_id").orderBy(F.col("greatest_prev_quantity_next_price_rounded").desc())
w4 = Window.partitionBy("supplier_id").orderBy(F.col("quantity").desc())
w5 = Window.orderBy("next_quantity", "id")

with_latest = (
    df_products
    .alias("s")
    .join(df_order_items.alias("c"), (F.col("c.product_id") == F.col("s.id")), "inner")
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("s.*", F.col("c.unit_price"), F.col("c.quantity"))
)

kept = (
    with_latest
    .alias("s")
    .join(df_store_sales.alias("x"), (F.col("x.product_id") == F.col("s.id")) & (F.col("x.sale_date") < F.col("s.launch_date")), "left_anti")
)

scored = (
    kept
    .filter(F.col("quantity") != 3)
    .select(
        "*",
        F.lead("quantity").over(w2).alias("next_quantity"),
        F.lag("quantity").over(w2).alias("prev_quantity"),
        F.lead("price").over(w2).alias("next_price"),
        F.lag("price").over(w2).alias("prev_price"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

valid = (
    scored
    .filter((F.col("launch_date") > F.lit("2023-06-09").cast("date")) & F.col("brand").isNotNull())
    .withColumn("greatest_prev_quantity_next_price", F.greatest("prev_quantity", "next_price"))
    .withColumn("product_name_filled", F.coalesce("product_name", F.lit("unassigned")))
)

base = (
    valid
    .filter(F.col("supplier_id").isNull() & F.col("brand").isNotNull())
    .select(
        "id",
        "product_name",
        "category",
        "brand",
        "price",
        "cost",
        "stock_qty",
        "tags",
        "supplier_id",
        "unit_price",
        "quantity",
        "next_quantity",
        "prev_quantity",
        "next_price",
        "row_seq",
        "product_name_filled",
        F.round("greatest_prev_quantity_next_price", 1).alias("greatest_prev_quantity_next_price_rounded"),
    )
)

scored_products = (
    base
    .select(
        "*",
        F.cume_dist().over(w3).alias("greatest_prev_quantity_next_price_rounded_cume_dist"),
        F.rank().over(w4).alias("quantity_rank"),
        F.lag("next_price", 2).over(w5).alias("prev_next_price"),
    )
)

valid_products = (
    scored_products
    .filter(~F.col("brand").eqNullSafe(F.lit("Acme")))
    .select(
        "*",
        F.round((F.col("prev_next_price") * 100), 2).alias("prev_next_price_scaled"),
        F.coalesce("brand", F.lit("NONE")).alias("brand_filled"),
    )
)

result = (
    valid_products
    .select(
        "id",
        "supplier_id",
        "next_price",
        F.col("greatest_prev_quantity_next_price_rounded_cume_dist").alias("GREATEST_PREV_QUANTITY_NEXT_PRICE_ROUNDED_CUME_DIST_VAL"),
        F.col("prev_next_price").alias("PREV_NEXT_PRICE"),
    )
    .unpivot(
        ids=["id", "supplier_id"],
        values=["next_price", "GREATEST_PREV_QUANTITY_NEXT_PRICE_ROUNDED_CUME_DIST_VAL", "PREV_NEXT_PRICE"],
        variableColumnName="measure",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
