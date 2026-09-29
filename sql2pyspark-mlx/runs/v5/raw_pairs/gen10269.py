from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.orderBy("price_whole_plus_quantity", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w2 = Window.orderBy("quantity_whole", "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.lit("2023-06-12").cast("date"))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        F.date_trunc("month", "last_restock").alias("last_restock_period"),
    )
)

enriched = (
    base
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .filter(F.col("p.launch_date") < "2024-10-01")
    .select("s.*", F.col("p.price"), F.col("p.brand"), F.col("p.cost"))
)

valid = (
    enriched
    .filter((F.col("last_restock") >= F.lit("2023-05-21").cast("date")) & F.col("brand").isin(["Initech", "Umbrella", "Globex"]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "price",
        "brand",
        F.upper("brand").alias("brand_upper"),
    )
)

base_inventory = (
    valid
    .filter(F.col("last_restock") >= F.to_date(F.lit("2023-06-21")))
    .select(
        "id",
        "quantity",
        "last_restock",
        "price",
        "brand_upper",
        F.floor("price").alias("price_whole"),
    )
)

cleaned = (
    base_inventory
    .filter(F.col("last_restock") >= F.lit("2023-06-18").cast("date"))
    .select(
        "id",
        "quantity",
        "last_restock",
        "price",
        "brand_upper",
        "price_whole",
        F.floor("quantity").alias("quantity_whole"),
        (F.col("price_whole") + F.col("quantity")).alias("price_whole_plus_quantity"),
    )
)

enriched_inventory = (
    cleaned
    .select(
        "id",
        "price_whole",
        "price_whole_plus_quantity",
        F.max("quantity").over(w_running).alias("running_max_quantity"),
        F.sum("quantity").over(w_running).alias("running_sum_quantity"),
        F.min("quantity").over(w_running).alias("running_min_quantity"),
        F.row_number().over(w2).alias("row_seq"),
    )
)

result = (
    enriched_inventory
    .select(
        "id",
        "price_whole_plus_quantity",
        "running_sum_quantity",
        "running_min_quantity",
        "row_seq",
    )
    .orderBy(
        F.col("price_whole_plus_quantity").desc(),
        F.col("running_sum_quantity").desc(),
    )
)
