from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id", "store_id").orderBy(F.col("sale_date").desc_nulls_first(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue").desc(), "id")
w3 = Window.partitionBy("product_id").orderBy(F.col("rank_in_group").desc())
w4 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w5 = Window.orderBy("sale_date", "id")
w6 = Window.partitionBy("product_id").orderBy("dedup_rank", "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-08-24").cast("date"))
    .select(
        "*",
        F.round("revenue", 2).alias("revenue_rounded"),
        F.date_format("sale_date", "yyyy-MM").alias("sale_date_label"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

ranked_store_sales = (
    ranked
    .withColumn("rank_in_group", F.row_number().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

ranked_store_sales_2 = (
    ranked_store_sales
    .withColumn("rank_in_group_2", F.rank().over(w3))
    .filter(F.col("rank_in_group_2") == 1)
    .select("id", "product_id", "sale_date", "sale_date_label", "dedup_rank")
)

enriched = (
    ranked_store_sales_2
    .filter(F.col("product_id").isin([1, 6]))
    .withColumn("event_seq", F.row_number().over(w4))
    .withColumn("event_seq_2", F.row_number().over(w5))
    .withColumn("row_seq", F.row_number().over(w4))
    .withColumn("event_seq_3", F.row_number().over(w6))
)

enriched_store_sales = (
    enriched
    .alias("s")
    .join(df_products.alias("d"), F.col("s.product_id") == F.col("d.id"), "left")
    .select(
        "s.id",
        "s.product_id",
        "s.sale_date",
        "s.sale_date_label",
        "s.dedup_rank",
        "s.event_seq_2",
        "s.row_seq",
        "s.event_seq_3",
        F.col("d.launch_date"),
        F.col("d.price"),
    )
)

enriched_store_sales_2 = (
    enriched_store_sales
    .alias("a")
    .join(df_products.alias("p"), F.col("a.product_id") == F.col("p.id"), "left")
    .select(
        "a.id",
        "a.product_id",
        "a.sale_date_label",
        "a.dedup_rank",
        "a.event_seq_2",
        "a.launch_date",
        "a.price",
        F.col("p.launch_date").alias("product_launch_date"),
        F.col("p.category"),
    )
)

result = (
    enriched_store_sales_2
    .groupBy("product_id")
    .agg(
        F.min("price").alias("min_price"),
        F.count_if(F.col("category") == "Electronics").alias("electronics_events"),
        F.count(F.when(F.col("category") == "Home", 1)).alias("home_count"),
        F.sum(F.when(F.col("category") == "Home", F.col("price")).otherwise(0)).alias("home_price"),
        F.countDistinct("product_id").alias("distinct_product_ids"),
        F.max(F.when(F.col("category") == "Sports", F.col("event_seq_2"))).alias("sports_max_event_seq_2"),
    )
    .orderBy(F.col("product_id").asc_nulls_first())
)
