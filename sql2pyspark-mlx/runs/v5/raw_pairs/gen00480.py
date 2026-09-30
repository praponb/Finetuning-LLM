from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("category", "product_id").orderBy("units_filled", "id")
w2 = Window.partitionBy("category").orderBy(F.col("sale_date_plus_months").asc_nulls_last(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-02-12")))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        F.add_months("sale_date", 1).alias("sale_date_plus_months"),
    )
)

valid = (
    cleaned
    .filter(F.col("sale_date_plus_months") >= "2024-11-01")
    .select(
        "*",
        F.round("units", 1).alias("units_rounded"),
        F.coalesce("units", F.lit(0)).alias("units_filled"),
    )
)

valid_store_sales = (
    valid
    .filter(F.col("product_id").isin([2, 7]))
    .withColumn("sale_date_plus_months_label", F.date_format("sale_date_plus_months", "yyyy-MM-dd"))
)

enriched = (
    valid_store_sales
    .alias("s")
    .join(df_products.alias("p"), F.col("s.product_id") == F.col("p.id"), "left")
    .select("s.*", F.col("p.category"))
)

ranked = (
    enriched
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
    .select(
        "id",
        "product_id",
        "sale_date",
        "sale_date_plus_months",
        "sale_date_plus_months_label",
        "category",
    )
)

ranked_store_sales = (
    ranked
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked_store_sales
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date_plus_months"))
    .withColumn("activity_month", F.date_trunc("MONTH", "sale_date"))
    .groupingSets(
        [["quarter_start", "activity_month", "product_id"], ["quarter_start", "activity_month"]],
        "quarter_start",
        "activity_month",
        "product_id",
    )
    .agg(
        F.sort_array(F.collect_set("category")).alias("category_list"),
        F.sort_array(F.collect_set("category")).alias("category_list_2"),
        F.count(F.when(F.col("category") == "Electronics", 1)).alias("electronics_count"),
        F.size(F.collect_set("category")).alias("num_categories"),
    )
    .orderBy(F.col("activity_month").desc())
)
