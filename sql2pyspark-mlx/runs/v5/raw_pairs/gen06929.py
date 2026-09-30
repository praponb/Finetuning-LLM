from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy("last_restock", "id")
w2 = Window.partitionBy("product_id").orderBy(F.col("last_restock_minus_30d").desc(), "id")
w3 = Window.partitionBy("store_id").orderBy(F.col("days_since_last_restock").asc_nulls_first())
w4 = Window.partitionBy("store_id")
w5 = Window.orderBy("opened_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

valid = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2023-05-01")))
    .select(
        "id",
        "store_id",
        "product_id",
        "last_restock",
        F.date_sub("last_restock", 30).alias("last_restock_minus_30d"),
    )
)

deduped = (
    valid
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .drop("latest_rank")
)

latest = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

joined = (
    latest
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .filter(F.col("p.opened_date").between("2024-01-01", "2025-03-17"))
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.last_restock",
        "src.last_restock_minus_30d",
        "src.rn",
        F.col("p.store_name"),
        F.col("p.opened_date"),
    )
)

valid_inventory = (
    joined
    .filter(F.upper("store_name").like("%LD %"))
    .select(
        "id",
        "store_id",
        "last_restock",
        "last_restock_minus_30d",
        "rn",
        "store_name",
        "opened_date",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

scored = (
    valid_inventory
    .select(
        "id",
        "last_restock",
        "store_name",
        "days_since_last_restock",
        F.cume_dist().over(w3).alias("days_since_last_restock_cume_dist"),
        F.min("days_since_last_restock").over(w4).alias("store_id_min_days_since_last_restock"),
        F.last("days_since_last_restock", ignorenulls=True).over(w5).alias("last_known_days_since_last_restock"),
    )
)

base = (
    scored
    .filter(F.col("last_restock") > F.lit("2023-06-03").cast("date"))
    .select(
        "id",
        "last_restock",
        "store_name",
        "days_since_last_restock",
        "days_since_last_restock_cume_dist",
        "store_id_min_days_since_last_restock",
        "last_known_days_since_last_restock",
        F.upper("store_name").alias("store_name_upper"),
    )
)

result = (
    base
    .select(
        "id",
        "days_since_last_restock",
        "days_since_last_restock_cume_dist",
        "store_id_min_days_since_last_restock",
        "last_known_days_since_last_restock",
        "store_name_upper",
    )
    .orderBy(F.col("id").desc())
    .limit(10)
)
