from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w_running = Window.partitionBy("category").orderBy("days_since_sale_date", "id")
w3 = Window.partitionBy("category").orderBy(F.col("next_days_since_sale_date").desc_nulls_last(), "id")

deduped = (
    df_store_sales
    .withColumn("rn", F.row_number().over(w))
    .filter(F.col("rn") == 1)
    .select("id", "product_id", "sale_date", "units")
)

base = (
    deduped
    .filter(F.col("sale_date") >= F.lit("2023-02-27").cast("date"))
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.floor("units").alias("units_whole"),
    )
)

joined = (
    base
    .alias("src")
    .join(df_products.alias("b"), F.col("src.product_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.launch_date"), F.col("b.cost"), F.col("b.category"))
)

metrics = (
    joined
    .withColumn("next_days_since_sale_date", F.lead("days_since_sale_date").over(w_running))
    .withColumn("prev_days_since_sale_date", F.lag("days_since_sale_date").over(w_running))
    .withColumn("next_cost", F.lead("cost").over(w_running))
    .withColumn("event_seq", F.row_number().over(w_running))
)

ranked = (
    metrics
    .withColumn("row_num", F.row_number().over(w3))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .groupBy(
        F.date_trunc("week", "sale_date").alias("week_start"),
        F.date_trunc("month", "launch_date").alias("activity_month"),
    )
    .agg(
        F.round(F.sum("units_whole") / F.nullif(F.count(F.when(F.col("category") == "Books", 1)), F.lit(0)), 2).alias("units_whole_per_books"),
        F.count_if(F.col("category") == "Home").alias("home_events"),
    )
    .orderBy(F.col("week_start").asc_nulls_first())
)
