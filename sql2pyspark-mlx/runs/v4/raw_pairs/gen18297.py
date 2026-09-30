from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy(F.col("units").desc(), "id")
w2 = Window.partitionBy("product_id").orderBy("units", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.partitionBy("product_id", "store_id").orderBy(F.col("units").desc_nulls_last())
w4 = Window.partitionBy("product_id").orderBy(F.col("seq_no").desc_nulls_last())
w5 = Window.partitionBy("store_id").orderBy("seq_no_times_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("product_id", "category").orderBy(F.col("price").desc(), "id")
w7 = Window.partitionBy("store_id")

enriched = (
    df_store_sales
    .select(
        "*",
        F.row_number().over(w).alias("seq_no"),
        F.sum("units").over(w2).alias("cumulative_units"),
        F.rank().over(w3).alias("units_rank"),
    )
)

cleaned = (
    enriched
    .filter(F.col("sale_date") > F.lit("2023-04-08").cast("date"))
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        "seq_no",
        "cumulative_units",
        F.col("units").cast("double").alias("units_str"),
        (F.col("seq_no") * F.col("revenue")).alias("seq_no_times_revenue"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w4))
    .filter(F.col("dedup_rank") == 1)
)

with_dims = (
    ranked
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.price"), F.col("p.cost"), F.col("p.category"))
)

with_dims_store_sales = (
    with_dims
    .alias("a")
    .join(df_stores.alias("p"), F.col("a.store_id") == F.col("p.id"), "left")
    .select("a.*", F.col("p.opened_date"))
)

scored = (
    with_dims_store_sales
    .withColumn("first_known_cost", F.first("cost", ignorenulls=True).over(w5))
    .withColumn("event_seq", F.row_number().over(w6))
    .withColumn("store_id_min_units", F.min("units").over(w7))
)

with_dims_store_sales_2 = (
    scored
    .alias("s")
    .join(df_stores.alias("d"), F.col("s.store_id") == F.col("d.id"), "inner")
    .select("s.*", F.col("d.sqft"), F.col("d.opened_date").alias("store_opened_date"))
)

result = (
    with_dims_store_sales_2
    .select(
        "id",
        "store_id",
        "product_id",
        "units",
        "seq_no",
        "cumulative_units",
        "units_str",
        "cost",
        "event_seq",
    )
)
