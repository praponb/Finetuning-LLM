from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("region").orderBy(F.col("opened_date").asc_nulls_last(), "id")

filtered = (
    df_stores
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-08-19"))) & F.col("region").isNotNull())
    .select(
        "id",
        "store_name",
        "region",
        "opened_date",
        "sqft",
        F.datediff(F.current_date(), "opened_date").alias("days_since_opened_date"),
    )
)

ranked = (
    filtered
    .withColumn("latest_rank", F.row_number().over(w))
    .filter(F.col("latest_rank") == 1)
    .select("id", "store_name", "opened_date")
)

cleaned = (
    ranked
    .filter(F.col("opened_date") >= F.lit("2023-08-09").cast("date"))
    .withColumn("opened_date_label", F.date_format("opened_date", "yyyy-MM-dd"))
    .withColumn("opened_date_label_2", F.date_format("opened_date", "yyyy-MM"))
)

base = (
    cleaned
    .filter(F.col("opened_date").between("2024-04-01", "2025-05-01"))
    .select("*", F.last_day("opened_date").alias("opened_date_month_end"))
)

result = (
    base
    .groupBy(F.date_trunc("QUARTER", "opened_date").alias("quarter_start"))
    .agg(
        F.count("*").alias("num_rows"),
        F.countDistinct("id").alias("distinct_ids"),
        F.countDistinct("id").alias("distinct_ids_2"),
    )
    .filter(F.col("distinct_ids_2") > 1)
    .orderBy(F.col("num_rows").desc())
)
