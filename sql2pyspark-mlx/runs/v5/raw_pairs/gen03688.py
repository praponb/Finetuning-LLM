from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("quantity").desc(), "id")
w2 = Window.partitionBy("city").orderBy(F.col("quantity_num").desc(), "id")
w_running = Window.partitionBy("region", "city").orderBy("opened_date", "id")
w4 = Window.partitionBy("region", "store_id").orderBy("adjusted_quantity_num", "id")
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

valid = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-09-25").cast("date")) & (F.col("quantity") != 83))
    .withColumn("quantity_num", F.col("quantity").cast("double"))
)

ranked = (
    valid
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select("id", "store_id", "last_restock", "quantity_num")
)

joined = (
    ranked
    .alias("e")
    .join(df_stores.alias("b"), F.col("e.store_id") == F.col("b.id"), "inner")
    .select("e.*", F.col("b.region"), F.col("b.opened_date"), F.col("b.city"))
)

filtered = (
    joined
    .filter((F.col("last_restock") > F.to_date(F.lit("2023-08-15"))) & F.col("region").isNotNull())
    .select(
        "*",
        F.add_months("opened_date", 6).alias("opened_date_plus_months"),
        F.round((F.col("quantity_num") * 1.1), 2).alias("adjusted_quantity_num"),
    )
)

with_windows = (
    filtered
    .withColumn("quantity_num_bucket", F.ntile(4).over(w2))
    .withColumn("next_adjusted_quantity_num", F.lead("adjusted_quantity_num", 1).over(w_running))
    .withColumn("prev_adjusted_quantity_num", F.lag("adjusted_quantity_num", 2).over(w4))
)

with_dims = (
    with_windows
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select(
        "src.id",
        "src.store_id",
        "src.last_restock",
        "src.region",
        "src.opened_date",
        "src.city",
        "src.quantity_num_bucket",
        "src.next_adjusted_quantity_num",
        F.col("b.city").alias("store_city"),
    )
)

with_dims_inventory = (
    with_dims
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "inner")
    .select(
        "a.id",
        "a.store_id",
        "a.last_restock",
        "a.region",
        "a.city",
        F.col("s.opened_date").alias("store_opened_date"),
        F.col("s.sqft"),
        F.col("s.city").alias("store_city_2"),
    )
)

result = (
    with_dims_inventory
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "last_restock",
        "region",
        "city",
        "store_opened_date",
        "sqft",
        "store_city_2",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("last_restock").desc())
)
