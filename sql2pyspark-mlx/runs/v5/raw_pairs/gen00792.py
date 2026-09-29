from pyspark.sql import functions as F

cleaned = (
    df_store_sales
    .filter(F.col("units") == 29)
    .withColumn("revenue_per_unit", F.round((F.col("revenue") / F.col("units")), 2))
)

result = (
    cleaned
    .select(
        "id",
        "store_id",
        F.col("revenue").alias("REVENUE"),
        F.col("revenue_per_unit").alias("REVENUE_PER_UNIT"),
    )
    .unpivot(
        ids=["id", "store_id"],
        values=["REVENUE", "REVENUE_PER_UNIT"],
        variableColumnName="metric",
        valueColumnName="reading",
    )
    .filter(F.col("reading").isNotNull())
)
