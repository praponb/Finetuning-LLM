from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("last_restock").asc_nulls_last())
w2 = Window.orderBy(F.col("quantity").asc_nulls_last())
w3 = Window.partitionBy("store_id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

base = (
    df_inventory
    .filter((F.col("last_restock") >= F.to_date(F.lit("2023-03-05"))) & F.col("quantity").isNull())
    .withColumn("last_restock_plus_months", F.add_months("last_restock", 6))
)

deduped = (
    base
    .withColumn("rn", F.rank().over(w))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

valid = (
    deduped
    .filter(F.col("last_restock") <= "2025-08-01")
    .withColumn("last_restock_plus_months_month_end", F.last_day("last_restock_plus_months"))
)

with_windows = (
    valid
    .withColumn("quantity_percent_rank", F.percent_rank().over(w2))
    .withColumn("store_id_avg_quantity", F.avg("quantity").over(w3))
)

summary = (
    with_windows
    .groupBy(
        "product_id",
        F.when(F.col("quantity_percent_rank") >= 0.84, "PREMIUM").otherwise("STANDARD").alias("quantity_percent_rank_tier"),
        F.date_trunc("year", "last_restock").alias("year_start"),
    )
    .agg(
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.sum("quantity").alias("total_quantity"),
    )
)

result = (
    summary
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "product_id",
        "quantity_percent_rank_tier",
        "year_start",
        "distinct_ids",
        "distinct_product_ids",
        "total_quantity",
        F.col("min_store_sale_units"),
    )
)
