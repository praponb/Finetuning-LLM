from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id")
w2 = Window.partitionBy("store_id").orderBy(F.col("corr_revenue_units").desc_nulls_first())
w3 = Window.partitionBy("store_id", "product_id").orderBy(F.col("revenue").asc_nulls_first())
w4 = Window.partitionBy("store_id").orderBy("sale_date", "id").rowsBetween(Window.unboundedPreceding, Window.currentRow)
w5 = Window.partitionBy("store_id").orderBy(F.col("units").desc())
w6 = Window.partitionBy("store_id").orderBy("sale_date_period", "id").rowsBetween(-3, -1)
revenue_by_store_id = df_store_sales.groupBy("store_id").agg(F.avg("revenue").alias("avg_store_sale_revenue"))

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") >= F.to_date(F.lit("2024-02-16")))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
)

enriched = (
    cleaned
    .select(
        "*",
        F.corr("revenue", "units").over(w).alias("corr_revenue_units"),
        F.round((F.col("units") / F.sum("units").over(w)), 4).alias("units_share"),
    )
)

latest = (
    enriched
    .withColumn("rank_in_group", F.rank().over(w2))
    .filter(F.col("rank_in_group") == 1)
)

latest_store_sales = (
    latest
    .withColumn("row_num", F.rank().over(w3))
    .filter(F.col("row_num") == 1)
)

valid = (
    latest_store_sales
    .filter(F.col("rank_in_group") != 1)
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        "days_since_sale_date",
        "corr_revenue_units",
        "units_share",
        "row_num",
        F.date_trunc("week", "sale_date").alias("sale_date_period"),
    )
)

metrics = (
    valid
    .withColumn("first_known_units_share", F.first("units_share", ignorenulls=True).over(w4))
    .withColumn("units_percent_rank", F.percent_rank().over(w5))
    .withColumn("revenue_avg_3_prior", F.avg("revenue").over(w6))
)

grouped = (
    metrics
    .withColumn("quarter_start", F.date_trunc("QUARTER", "sale_date"))
    .rollup("store_id", "quarter_start")
    .agg(
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("span_minutes"),
        F.round(F.avg("corr_revenue_units"), 2).alias("avg_corr_revenue_units"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("span_minutes_2"),
        F.percentile_approx("units_percent_rank", 0.5).alias("units_percent_rank_p50"),
        F.bit_and("units").alias("units_bit_and"),
        ((F.unix_timestamp(F.max("sale_date_period")) - F.unix_timestamp(F.min("sale_date_period"))) / 60).alias("span_minutes_3"),
        F.grouping("store_id").alias("grouping_id"),
    )
    .filter(F.col("span_minutes_2") > 0)
)

result = (
    grouped
    .join(revenue_by_store_id, on="store_id", how="left")
    .select(
        "store_id",
        "span_minutes",
        "avg_corr_revenue_units",
        "units_percent_rank_p50",
        "units_bit_and",
        "span_minutes_3",
        "grouping_id",
        F.col("avg_store_sale_revenue"),
    )
    .orderBy(F.col("span_minutes_3").desc())
)
