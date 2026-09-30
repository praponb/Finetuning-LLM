from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("revenue", "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy("units", "id")
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_rounded").desc_nulls_last())
w_recent = Window.orderBy(F.col("next_units").desc(), "id")
w5 = Window.partitionBy("product_id").orderBy("revenue_rounded", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.orderBy("revenue_rounded", "id")

cleaned = (
    df_store_sales
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-05-22"))) & (F.col("store_id") == 2))
    .withColumn("revenue_rounded", F.round("revenue", 0))
)

with_windows = (
    cleaned
    .select(
        "id",
        "store_id",
        "product_id",
        "revenue_rounded",
        F.lag("revenue", 1, 0).over(w).alias("prev_revenue"),
        F.lead("units", 1).over(w2).alias("next_units"),
    )
)

with_dims = (
    with_windows
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "inner")
    .select("a.*", F.col("p.opened_date"), F.col("p.city"), F.col("p.store_name"))
)

ranked = (
    with_dims
    .withColumn("rn", F.rank().over(w3))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "revenue_rounded", "next_units", "opened_date")
)

scored = (
    ranked
    .select(
        "*",
        F.row_number().over(w_recent).alias("row_seq"),
        F.sum("next_units").over(w5).alias("cumulative_next_units"),
        F.lead("next_units", 1).over(w6).alias("next_next_units"),
    )
)

result = (
    scored
    .alias("a")
    .join(df_products.alias("d"), F.col("a.product_id") == F.col("d.id"), "inner")
    .groupingSets(
        [["brand", "category"], ["brand"]],
        F.col("d.brand").alias("brand"),
        F.col("d.category").alias("category"),
    )
    .agg(
        F.count("*").alias("num_rows"),
        F.round(F.sum(F.col("a.next_units")), 2).alias("total_next_units"),
        F.max(F.col("a.revenue_rounded")).alias("max_revenue_rounded"),
        F.grouping_id().alias("level_id"),
    )
    .orderBy(F.col("brand").asc_nulls_first())
)
