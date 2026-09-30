from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("revenue").desc())
w2 = Window.orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w3 = Window.orderBy("revenue", "id")
w4 = Window.partitionBy("product_id")
w5 = Window.partitionBy("product_id").orderBy(F.col("revenue").asc_nulls_last())
w_ordered = Window.partitionBy("product_id").orderBy(F.col("running_revenue").desc(), "id")
w_recent = Window.partitionBy("product_id").orderBy("running_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w8 = Window.partitionBy("product_id").orderBy(F.col("sale_date").desc(), "id")
w9 = Window.partitionBy("product_id").orderBy("revenue_rank", "id")

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2024-01-26")))
    .select(
        "id",
        "product_id",
        "sale_date",
        "units",
        "revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
    )
)

scored = (
    filtered
    .select(
        "*",
        F.rank().over(w).alias("revenue_rank"),
        F.sum("revenue").over(w2).alias("running_revenue"),
        F.lag("units", 1).over(w3).alias("prev_units"),
        F.round((F.col("revenue") / F.sum("revenue").over(w4)), 4).alias("revenue_share"),
        F.cume_dist().over(w5).alias("revenue_cume_dist"),
    )
)

enriched = (
    scored
    .withColumn("seq_no", F.row_number().over(w_ordered))
    .withColumn("row_seq", F.row_number().over(w_ordered))
    .withColumn("running_revenue_cume_dist", F.sum("revenue_cume_dist").over(w_recent))
    .withColumn("event_seq", F.row_number().over(w8))
    .withColumn("event_seq_2", F.row_number().over(w9))
)

result = (
    enriched
    .select("id", "product_id", "revenue_share", "revenue_cume_dist", "revenue")
    .unpivot(
        ["id", "product_id"],
        ["revenue_share", "revenue_cume_dist", "revenue"],
        "metric",
        "value",
    )
)
