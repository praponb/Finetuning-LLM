from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy("revenue_filled")
w2 = Window.partitionBy("city").orderBy(F.col("revenue_filled").desc())
w3 = Window.partitionBy("product_id", "store_id").orderBy("dedup_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("revenue").desc(), "id")
w5 = Window.partitionBy("product_id", "city").orderBy("revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w6 = Window.partitionBy("city").orderBy(F.col("first_known_revenue_filled").desc(), "id")
w7 = Window.partitionBy("city", "product_id").orderBy(F.col("opened_date").desc(), "id")
w8 = Window.orderBy("opened_date", "id").rowsBetween(-6, Window.currentRow)
w9 = Window.partitionBy("product_id")
w10 = Window.partitionBy("product_id").orderBy("revenue_filled_dense_rank", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w_ordered = Window.partitionBy("product_id").orderBy("first_known_revenue_filled", "id")
w_grp = Window.partitionBy("city", "product_id").orderBy("latest_rank", "id")
units_by_product_id = df_store_sales.groupBy("product_id").agg(F.min("units").alias("min_store_sale_units"))

cleaned = (
    df_store_sales
    .filter(F.col("product_id").isin([3, 7, 8]))
    .select("*", F.coalesce("revenue", F.lit(0)).alias("revenue_filled"))
)

latest = (
    cleaned
    .withColumn("dedup_rank", F.rank().over(w))
    .filter(F.col("dedup_rank") == 1)
)

enriched = (
    latest
    .alias("src")
    .join(df_stores.alias("b"), F.col("src.store_id") == F.col("b.id"), "left")
    .select("src.*", F.col("b.city"), F.col("b.opened_date"), F.col("b.store_name"))
)

scored = (
    enriched
    .select(
        "id",
        "product_id",
        "revenue",
        "city",
        "opened_date",
        "store_name",
        F.dense_rank().over(w2).alias("revenue_filled_dense_rank"),
        F.last("city", ignorenulls=True).over(w3).alias("last_known_city"),
        F.row_number().over(w4).alias("row_seq"),
        F.first("revenue_filled", ignorenulls=True).over(w5).alias("first_known_revenue_filled"),
    )
)

ranked = (
    scored
    .withColumn("latest_rank", F.row_number().over(w6))
    .filter(F.col("latest_rank") == 1)
)

ranked_store_sales = (
    ranked
    .withColumn("rn", F.row_number().over(w7))
    .filter(F.col("rn") == 1)
    .drop("rn")
)

metrics = (
    ranked_store_sales
    .select(
        "id",
        "product_id",
        "revenue",
        "city",
        "opened_date",
        "store_name",
        "revenue_filled_dense_rank",
        "latest_rank",
        F.avg("first_known_revenue_filled").over(w8).alias("first_known_revenue_filled_avg_6"),
        F.count("revenue_filled_dense_rank").over(w9).alias("product_id_count_revenue_filled_dense_rank"),
        F.sum("first_known_revenue_filled").over(w10).alias("cumulative_first_known_revenue_filled"),
        F.ntile(3).over(w_ordered).alias("first_known_revenue_filled_bucket"),
        F.row_number().over(w_grp).alias("row_seq_2"),
    )
)

result = (
    metrics
    .join(units_by_product_id, on="product_id", how="left")
    .select(
        "id",
        "product_id",
        "revenue",
        "city",
        "opened_date",
        "store_name",
        "revenue_filled_dense_rank",
        "product_id_count_revenue_filled_dense_rank",
        "cumulative_first_known_revenue_filled",
        F.col("min_store_sale_units"),
    )
    .orderBy(
        F.col("cumulative_first_known_revenue_filled").desc(),
        F.col("revenue_filled_dense_rank").desc(),
    )
)
