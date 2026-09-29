from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_part = Window.partitionBy("order_id").orderBy("quantity", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("quantity").desc_nulls_first(), "id")

valid = (
    df_order_items
    .filter(F.col("product_id").isNotNull() & F.col("order_id").isin([24, 34, 38]))
    .select(
        "id",
        "order_id",
        "product_id",
        "quantity",
        F.col("unit_price").cast("int").alias("unit_price_int"),
    )
)

with_windows = (
    valid
    .select(
        "id",
        "product_id",
        "quantity",
        "unit_price_int",
        F.lead("unit_price_int").over(w_part).alias("next_unit_price_int"),
        F.lead("quantity").over(w_part).alias("next_quantity"),
        F.row_number().over(w_part).alias("row_seq"),
    )
)

cleaned = (
    with_windows
    .filter(F.col("product_id").isNotNull())
    .withColumn("next_unit_price_int_whole", F.ceil("next_unit_price_int"))
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w2))
    .filter(F.col("dedup_rank") == 1)
    .drop("dedup_rank")
)

enriched = (
    latest
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.stock_qty") <= 207)
    .select("src.*", F.col("p.brand"))
)

cleaned_order_items = (
    enriched
    .filter(F.col("product_id").isin([5, 21]))
    .select(
        "id",
        "product_id",
        "quantity",
        "next_unit_price_int",
        "next_unit_price_int_whole",
        (F.col("next_unit_price_int") * F.col("row_seq")).alias("next_unit_price_int_times_row_seq"),
        F.length("brand").alias("brand_length"),
    )
)

with_dims = (
    cleaned_order_items
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id").eqNullSafe(F.col("p.id")), "left")
    .select("a.*", F.col("p.price"), F.col("p.brand"), F.col("p.category"))
)

result = (
    with_dims
    .select(
        "id",
        "next_unit_price_int_times_row_seq",
        "brand",
        "category",
        F.when(F.col("brand").rlike("^(Umbrella|Acme)$"), "GROUP_B").otherwise("OTHER_ZONE").alias("brand_zone"),
    )
    .orderBy(F.col("category").desc())
)
