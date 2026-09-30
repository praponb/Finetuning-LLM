from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("product_id").orderBy("days_since_sale_date_2", "id")
w2 = Window.orderBy(F.col("revenue").desc(), "id")
w3 = Window.orderBy("days_since_sale_date_2", "id")
w4 = Window.partitionBy("product_id").orderBy(F.col("days_since_sale_date").asc_nulls_last())
w5 = Window.partitionBy("store_id").orderBy("units", "id")
w6 = Window.partitionBy("store_id").orderBy(F.col("days_since_sale_date_2").desc())
quantity_by_store_id = df_inventory.groupBy("store_id").agg(F.avg("quantity").alias("avg_inventory_quantity"))

filtered = (
    df_store_sales
    .filter(F.col("store_id").isin([2, 3, 4]))
    .select(
        "*",
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date"),
        F.datediff(F.current_date(), "sale_date").alias("days_since_sale_date_2"),
    )
)

enriched = (
    filtered
    .filter(F.col("days_since_sale_date_2") > 73)
    .select(
        "*",
        F.row_number().over(w).alias("row_seq"),
        F.row_number().over(w2).alias("row_seq_2"),
        F.lag("units", 2, 0).over(w3).alias("prev_units"),
        F.percent_rank().over(w4).alias("days_since_sale_date_percent_rank"),
        F.row_number().over(w5).alias("event_seq"),
    )
)

base = (
    enriched
    .filter(F.col("sale_date") >= F.lit("2023-06-19").cast("date"))
    .withColumn("greatest_row_seq_days_since_sale_date", F.greatest("row_seq", "days_since_sale_date"))
)

ranked = (
    base
    .withColumn("latest_rank", F.rank().over(w6))
    .filter(F.col("latest_rank") == 1)
)

totals = (
    ranked
    .rollup("store_id")
    .agg(
        F.sum("days_since_sale_date").alias("total_days_since_sale_date"),
        F.countDistinct("id").alias("distinct_ids"),
    )
)

result = (
    totals
    .join(quantity_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "total_days_since_sale_date",
        "distinct_ids",
        F.col("avg_inventory_quantity"),
    )
    .orderBy(F.col("avg_inventory_quantity").desc_nulls_first(), "distinct_ids")
)
