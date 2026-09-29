from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("customer_id", "status").orderBy("start_date", "id")
w2 = Window.partitionBy("customer_id").orderBy("start_date", "id").rowsBetween(-2, Window.currentRow)
w3 = Window.orderBy(F.col("end_date_year").asc_nulls_first())
w4 = Window.partitionBy("plan").orderBy("row_seq", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("plan")
w_grp = Window.partitionBy("plan").orderBy(F.col("end_date_year_percent_rank").desc_nulls_first())

base = (
    df_subscriptions
    .filter(F.col("plan").isin(["enterprise", "standard", "premium"]))
    .select(
        "id",
        "customer_id",
        "plan",
        "start_date",
        "end_date",
        "status",
        F.year("end_date").alias("end_date_year"),
    )
)

scored = (
    base
    .select(
        "*",
        F.lag("end_date_year").over(w).alias("prev_end_date_year"),
        F.lead("end_date_year").over(w).alias("next_end_date_year"),
        F.row_number().over(w).alias("row_seq"),
    )
)

enriched = (
    scored
    .select(
        "id",
        "plan",
        "end_date",
        "prev_end_date_year",
        "row_seq",
        F.avg("end_date_year").over(w2).alias("end_date_year_avg_2"),
        F.percent_rank().over(w3).alias("end_date_year_percent_rank"),
        F.last("prev_end_date_year", ignorenulls=True).over(w4).alias("last_known_prev_end_date_year"),
    )
)

metrics = (
    enriched
    .select(
        "id",
        "plan",
        "row_seq",
        "end_date_year_avg_2",
        "end_date_year_percent_rank",
        F.avg("last_known_prev_end_date_year").over(w5).alias("plan_avg_last_known_prev_end_date_year"),
        F.percent_rank().over(w_grp).alias("end_date_year_percent_rank_percent_rank"),
    )
)

result = (
    metrics
    .groupBy(
        F.when(F.col("end_date_year_avg_2") >= 72, "HIGH").otherwise("LOW").alias("end_date_year_avg_2_tier"),
        F.when(F.col("end_date_year_percent_rank") >= 0.89, "LARGE").otherwise("SMALL").alias("end_date_year_percent_rank_tier"),
    )
    .agg(
        F.sort_array(F.collect_set("plan")).alias("plan_list"),
        F.count(F.when(F.col("plan") == "premium", 1)).alias("premium_count"),
        F.round(F.avg("plan_avg_last_known_prev_end_date_year"), 2).alias("avg_plan_avg_last_known_prev_end_date_year"),
    )
    .filter(F.col("avg_plan_avg_last_known_prev_end_date_year") > 0)
    .orderBy(F.col("avg_plan_avg_last_known_prev_end_date_year").desc_nulls_first())
)
