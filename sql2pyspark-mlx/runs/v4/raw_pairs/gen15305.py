from pyspark.sql import functions as F

result = (
    df_shipments
    .groupBy(
        F.when(F.year("shipped_date") < 2024, "before_2024").when(F.year("shipped_date") == 2024, "2024").otherwise("after_2024").alias("shipped_cohort"),
    )
    .agg(
        F.count("cost").alias("cost_count"),
        F.count("*").alias("row_count"),
    )
    .filter(F.col("cost_count") < 4)
    .select("shipped_cohort", "cost_count", "row_count")
    .orderBy("cost_count")
)
