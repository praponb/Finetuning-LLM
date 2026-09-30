from pyspark.sql import functions as F
from pyspark.sql.window import Window

w_running = Window.partitionBy("product_id").orderBy("sale_date_month_end", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= "2023-03-01")
    .select(
        "*",
        F.when(F.col("units") >= 44, "High").when(F.col("units") >= 8, "Medium").otherwise("Low").alias("units_tier"),
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

enriched = (
    cleaned
    .alias("src")
    .join(df_stores.alias("p"), F.col("src.store_id") == F.col("p.id"), "inner")
    .filter(F.col("p.sqft").between(12996, 13700))
    .select(
        "src.id",
        "src.store_id",
        "src.product_id",
        "src.sale_date",
        "src.revenue",
        "src.sale_date_month_end",
        F.col("p.opened_date"),
        F.col("p.sqft"),
        F.col("p.store_name"),
    )
)

enriched_store_sales = (
    enriched
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .select(
        "a.*",
        F.col("s.city"),
        F.col("s.sqft").alias("store_sqft"),
        F.col("s.store_name").alias("store_store_name"),
    )
)

metrics = (
    enriched_store_sales
    .withColumn("next_sqft", F.lead("sqft").over(w_running))
    .withColumn("next_store_sqft", F.lead("store_sqft").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
)

result = (
    metrics
    .alias("a")
    .join(df_stores.alias("s"), F.col("a.store_id") == F.col("s.id"), "left")
    .groupingSets(
        [["store_city", "store_sale_city"], ["store_city"], []],
        F.col("s.city").alias("store_city"),
        F.col("a.city").alias("store_sale_city"),
    )
    .agg(
        F.count("*").alias("rows_total"),
        F.max(F.col("a.next_store_sqft")).alias("max_next_store_sqft"),
    )
)
