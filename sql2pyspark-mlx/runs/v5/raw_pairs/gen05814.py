from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_grp = Window.partitionBy("product_id").orderBy("cost", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-13").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "revenue",
        (F.col("revenue") + F.col("units")).alias("revenue_per_unit"),
        F.round((F.col("revenue") * 100), 2).alias("adjusted_revenue"),
    )
)

with_dims = (
    cleaned
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "inner")
    .select("src.*", F.col("b.city"), F.col("b.opened_date"), F.col("b.sqft"))
)

enriched = (
    with_dims
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .filter(F.array_contains(F.col("p.tags"), "sale"))
    .select("a.id", "a.product_id", F.col("p.cost"))
)

enriched_store_sales = (
    enriched
    .withColumn("next_cost", F.lead("cost", 2).over(w_grp))
    .withColumn("row_seq", F.row_number().over(w_grp))
)

result = (
    enriched_store_sales
    .select("id", "product_id", "cost")
    .orderBy("id", "cost")
)
