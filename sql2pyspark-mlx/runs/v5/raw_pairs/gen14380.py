from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("last_restock").asc_nulls_first(), "id")

base = (
    df_inventory
    .filter((F.col("last_restock") > F.lit("2023-03-18").cast("date")) & F.col("product_id").isin([6, 7, 8, 9]))
    .select("*", F.date_add("last_restock", 17).alias("last_restock_plus_17d"))
)

joined = (
    base
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.sqft"))
)

base_inventory = (
    joined
    .filter(F.col("product_id") == 2)
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "sqft",
        F.add_months("last_restock_plus_17d", 12).alias("last_restock_plus_17d_plus_months"),
        F.date_trunc("week", "last_restock").alias("last_restock_period"),
    )
)

valid = (
    base_inventory
    .filter(F.col("product_id").isin([4, 6, 9]))
    .select(
        "id",
        "product_id",
        "quantity",
        "last_restock",
        "sqft",
        F.datediff(F.current_date(), "last_restock_plus_17d_plus_months").alias("days_since_last_restock_plus_17d_plus_months"),
        F.date_trunc("year", "last_restock").alias("last_restock_period_2"),
    )
)

ranked = (
    valid
    .withColumn("rank_in_group", F.row_number().over(w))
    .filter(F.col("rank_in_group") == 1)
    .drop("rank_in_group")
)

result = (
    ranked
    .groupBy("product_id")
    .agg(
        F.stddev_samp("quantity").alias("quantity_stddev"),
        F.count("*").alias("num_rows"),
        F.sum("sqft").alias("total_sqft"),
    )
    .orderBy(F.col("product_id").desc())
)
