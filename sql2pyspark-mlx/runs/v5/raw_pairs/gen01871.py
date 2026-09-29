from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("store_id").orderBy(F.col("days_since_sale_date").desc(), "id")
w2 = Window.partitionBy("product_id", "store_id").orderBy(F.col("revenue_per_unit_2").desc(), "id")

cleaned = (
    df_store_sales
    .filter(F.col("sale_date") > F.lit("2023-09-03").cast("date"))
    .withColumn("revenue_per_unit", (F.col("units") * F.col("revenue")))
    .withColumn("revenue_per_unit_2", F.round((F.col("units") / F.col("revenue")), 2))
)

filtered = (
    cleaned
    .filter((F.col("sale_date") >= F.lit("2023-03-01").cast("date")) & F.col("product_id").isin([7, 9, 10]))
    .withColumn("days_since_sale_date", F.datediff(F.current_date(), "sale_date"))
    .withColumn("sale_date_plus_months", F.add_months("sale_date", 12))
)

latest = (
    filtered
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

latest_store_sales = (
    latest
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

filtered_store_sales = (
    latest_store_sales
    .filter(F.col("sale_date") >= F.lit("2023-09-22").cast("date"))
    .select(
        "id",
        "store_id",
        "sale_date",
        "revenue",
        "revenue_per_unit",
        "revenue_per_unit_2",
        "days_since_sale_date",
        "sale_date_plus_months",
        "rn",
        F.when(F.col("revenue_per_unit_2") >= 83, "High").when(F.col("revenue_per_unit_2") >= 16, "Medium").otherwise("Low").alias("revenue_per_unit_2_band"),
    )
)

result = (
    filtered_store_sales
    .select(
        "id",
        "store_id",
        "revenue_per_unit_2",
        F.col("revenue_per_unit").alias("REVENUE_PER_UNIT_VAL"),
        F.col("revenue").alias("REVENUE_VAL"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["revenue_per_unit_2", "REVENUE_PER_UNIT_VAL", "REVENUE_VAL"],
        variableColumnName="kpi",
        valueColumnName="val",
    )
    .filter(F.col("val").isNotNull())
)
