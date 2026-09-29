from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("sqft").desc_nulls_last(), "id")

base = (
    df_inventory
    .filter(F.col("last_restock") > F.to_date(F.lit("2024-01-21")))
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        F.datediff(F.current_date(), "last_restock").alias("days_since_last_restock"),
    )
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.sqft"), F.col("d.opened_date"), F.col("d.store_name"))
)

joined_inventory = (
    joined
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.opened_date").alias("store_opened_date"), F.col("p.city"))
)

ranked = (
    joined_inventory
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

result = (
    ranked
    .select(
        "id",
        "store_id",
        "product_id",
        "quantity",
        "last_restock",
        "sqft",
        "store_name",
        "store_opened_date",
        "city",
    )
    .orderBy(F.col("last_restock").desc())
)
