from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue_per_unit").desc(), "id")
w2 = Window.partitionBy("store_id", "category").orderBy("sale_date", "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("row_num").desc(), "id")
w_grp = Window.partitionBy("category", "store_id").orderBy("launch_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

base = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-07-15").cast("date"))
    .select("*", (F.col("units") + F.col("revenue")).alias("revenue_per_unit"))
)

cleaned = (
    base
    .filter(F.col("revenue") > 2000)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "units",
        "revenue_per_unit",
        F.when(F.col("revenue") >= 3600, "High").when(F.col("revenue") >= 1800, "Medium").otherwise("Low").alias("revenue_tier"),
    )
)

deduped = (
    cleaned
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

with_dims = (
    deduped
    .alias("src")
    .join(df_products.alias("d"), F.col("src.product_id") == F.col("d.id"), "inner")
    .select("src.*", F.col("d.launch_date"), F.col("d.category"), F.col("d.cost"))
)

scored = (
    with_dims
    .filter(F.col("units") <= 11)
    .select(
        "id",
        "store_id",
        "product_id",
        "sale_date",
        "row_num",
        "launch_date",
        "category",
        "cost",
        F.lead("revenue_per_unit").over(w2).alias("next_revenue_per_unit"),
        F.lag("cost").over(w2).alias("prev_cost"),
        F.row_number().over(w2).alias("seq_no"),
    )
)

ranked = (
    scored
    .withColumn("row_num_2", F.row_number().over(w3))
    .filter(F.col("row_num_2") == 1)
)

metrics = (
    ranked
    .withColumn("running_count_prev_cost", F.count("prev_cost").over(w_grp))
    .withColumn("running_max_prev_cost", F.max("prev_cost").over(w_grp))
    .withColumn("running_sum_prev_cost", F.sum("prev_cost").over(w_grp))
)

result = (
    metrics
    .rollup(
        "store_id",
        F.when(F.col("running_max_prev_cost") >= 610, "HIGH").otherwise("LOW").alias("running_max_prev_cost_tier"),
    )
    .agg(
        F.count("*").alias("event_count"),
        F.sum("running_sum_prev_cost").alias("total_running_sum_prev_cost"),
    )
    .orderBy("event_count")
)
