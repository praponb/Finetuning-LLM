from pyspark.sql import functions as F

result = (
    df_store_sales
    .filter(F.col("store_id").isin([1, 4, 5, 6]))
    .groupBy(
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
        "store_id",
    )
    .agg(
        F.round(F.avg("units"), 2).alias("avg_units"),
        (F.max("revenue") - F.min("revenue")).alias("revenue_range"),
        F.count("*").alias("num_store_sales"),
    )
    .filter(F.col("num_store_sales") >= 2)
    .drop("num_store_sales")
    .orderBy(F.col("store_id").desc(), "sale_cohort")
)
