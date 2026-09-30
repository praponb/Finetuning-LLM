from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.orderBy(F.col("sale_date_day").asc_nulls_first())
w2 = Window.orderBy(F.col("sale_date_day").asc_nulls_last())
w3 = Window.orderBy("sale_date_day", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy("sale_date_day", "id")

cleaned = (
    df_store_sales
    .filter(F.col("product_id").isin([4, 10]))
    .select("id", "revenue", F.dayofmonth("sale_date").alias("sale_date_day"))
)

enriched = (
    cleaned
    .filter(F.col("revenue").between(2900, 3000))
    .withColumn("sale_date_day_rank", F.rank().over(w))
    .withColumn("sale_date_day_rank_2", F.rank().over(w2))
    .withColumn("cumulative_revenue", F.sum("revenue").over(w3))
    .withColumn("seq_no", F.row_number().over(w4))
)

cleaned_store_sales = (
    enriched
    .filter(F.col("sale_date_day_rank") <= 5)
    .select("*", F.col("sale_date_day").cast("string").alias("sale_date_day_str"))
)

cleaned_store_sales_2 = (
    cleaned_store_sales
    .filter(F.col("sale_date_day_rank_2") < 7)
    .withColumn("cumulative_revenue_str", F.col("cumulative_revenue").cast("string"))
)

cleaned_store_sales_3 = (
    cleaned_store_sales_2
    .filter(F.col("seq_no") != 4)
    .select(
        "id",
        "revenue",
        "sale_date_day",
        "sale_date_day_rank",
        "sale_date_day_rank_2",
        "cumulative_revenue",
        "seq_no",
        "sale_date_day_str",
        F.floor("cumulative_revenue").alias("cumulative_revenue_whole"),
    )
)

result = (
    cleaned_store_sales_3
    .groupBy(
        F.when(F.col("cumulative_revenue") >= 19608, "LARGE").otherwise("SMALL").alias("cumulative_revenue_tier"),
    )
    .agg(
        F.round(F.avg("cumulative_revenue"), 2).alias("avg_cumulative_revenue"),
        F.percentile_approx("revenue", [0.25, 0.5, 0.75]).alias("revenue_quartiles"),
        F.count("*").alias("cnt"),
    )
    .orderBy(F.col("cumulative_revenue_tier").asc_nulls_last())
)
