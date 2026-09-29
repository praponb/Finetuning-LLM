from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_recent = Window.partitionBy("product_id").orderBy("units", "id")
w2 = Window.partitionBy("store_id").orderBy("units", "id").rowsBetween(-5, -1)
w3 = Window.partitionBy("product_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_running = Window.partitionBy("product_id").orderBy("revenue_rounded", "id")
w5 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue_rounded").desc_nulls_last())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.max("quantity").alias("max_inventory_quantity"))

valid = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-07-12").cast("date")) & F.col("store_id").isin([4, 5]))
    .select(
        "*",
        F.round("revenue", 0).alias("revenue_rounded"),
        F.col("revenue").cast("int").alias("revenue_int"),
    )
)

scored = (
    valid
    .select(
        "*",
        F.lead("revenue", 1).over(w_recent).alias("next_revenue"),
        F.avg("revenue_rounded").over(w2).alias("revenue_rounded_avg_5_prior"),
        F.sum("revenue_rounded").over(w3).alias("cumulative_revenue_rounded"),
        F.lead("units", 2).over(w_running).alias("next_units"),
        F.cume_dist().over(w5).alias("revenue_rounded_cume_dist"),
    )
)

with_dims = (
    scored
    .alias("src")
    .join(df_products.alias("p"), F.col("src.product_id") == F.col("p.id"), "left")
    .select("src.*", F.col("p.cost"), F.col("p.brand"))
)

result = (
    with_dims
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "revenue",
        "revenue_int",
        "next_revenue",
        "cumulative_revenue_rounded",
        "next_units",
        F.col("max_inventory_quantity"),
    )
    .orderBy(F.col("id").desc())
)
