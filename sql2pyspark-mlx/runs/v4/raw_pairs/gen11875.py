from pyspark.sql import functions as F

result = (
    df_store_sales
    .groupBy(
        "store_id",
        F.when(F.year("sale_date") < 2024, "before_2024").when(F.year("sale_date") == 2024, "2024").otherwise("after_2024").alias("sale_cohort"),
    )
    .agg(
        F.max("sale_date").alias("last_sale_date"),
        F.round(F.avg("units"), 2).alias("avg_units"),
        (F.max("units") - F.min("units")).alias("units_range"),
    )
    .filter(F.col("avg_units") > 71)
)
