from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").asc_nulls_first(), "id")
w_running = Window.orderBy("revenue", "id")
w_grp = Window.orderBy("next_revenue", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w4 = Window.orderBy(F.col("cumulative_row_seq").desc_nulls_last())
w5 = Window.orderBy("cumulative_row_seq", "id").rowsBetween(-6, -1)
w6 = Window.orderBy("next_revenue", "id").rowsBetween(-3, -1)
w7 = Window.orderBy(F.col("next_revenue").desc(), "id")
w8 = Window.orderBy("cumulative_next_revenue_avg_6_prior", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

filtered = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2023-09-13")))
    .select("*", F.last_day("sale_date").alias("sale_date_month_end"))
)

deduped = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
    .select("id", "sale_date", "revenue", "sale_date_month_end")
)

enriched = (
    deduped
    .withColumn("next_revenue", F.lead("revenue").over(w_running))
    .withColumn("seq_no", F.row_number().over(w_running))
    .withColumn("row_seq", F.row_number().over(w_running))
)

scored = (
    enriched
    .select(
        "*",
        F.sum("next_revenue").over(w_grp).alias("cumulative_next_revenue"),
        F.sum("row_seq").over(w_grp).alias("cumulative_row_seq"),
    )
)

metrics = (
    scored
    .withColumn("cumulative_row_seq_cume_dist", F.cume_dist().over(w4))
    .withColumn("cumulative_next_revenue_avg_6_prior", F.avg("cumulative_next_revenue").over(w5))
)

result = (
    metrics
    .select(
        "id",
        "sale_date",
        "revenue",
        "sale_date_month_end",
        "next_revenue",
        "seq_no",
        "row_seq",
        "cumulative_row_seq",
        "cumulative_next_revenue_avg_6_prior",
        F.avg("revenue").over(w6).alias("revenue_avg_3_prior"),
        F.row_number().over(w7).alias("row_seq_2"),
        F.sum("row_seq").over(w8).alias("running_row_seq"),
    )
)
