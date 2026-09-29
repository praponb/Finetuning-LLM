from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("revenue_whole_rounded").desc_nulls_last(), "id")

filtered = (
    df_store_sales
    .filter((F.col("sale_date") > F.lit("2023-08-11").cast("date")) & (F.col("units") <= 36))
    .select(
        "id",
        "store_id",
        "sale_date",
        "units",
        "revenue",
        F.floor("revenue").alias("revenue_whole"),
        F.least("units", "revenue").alias("least_units_revenue"),
    )
)

cleaned = (
    filtered
    .filter((F.col("sale_date") > F.to_date(F.lit("2023-08-23"))) & F.col("revenue_whole").isNull())
    .select(
        "id",
        "store_id",
        "units",
        "revenue",
        "revenue_whole",
        "least_units_revenue",
        F.last_day("sale_date").alias("sale_date_month_end"),
        F.round("revenue_whole", 0).alias("revenue_whole_rounded"),
    )
)

ranked = (
    cleaned
    .withColumn("dedup_rank", F.row_number().over(w))
    .filter(F.col("dedup_rank") == 1)
)

totals = (
    ranked
    .groupBy(F.date_trunc("QUARTER", "sale_date_month_end").alias("quarter_start"))
    .agg(
        F.sum("least_units_revenue").alias("total_least_units_revenue"),
        F.bit_and("units").alias("units_bit_and"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("store_id").alias("distinct_store_ids"),
        F.count("*").alias("cnt"),
    )
)

result = (
    totals
    .select(
        "quarter_start",
        "total_least_units_revenue",
        "units_bit_and",
        "distinct_ids",
    )
)
