from pyspark.sql import functions as F

result = (
    df_stores
    .groupBy(F.date_trunc("YEAR", "opened_date").alias("year_start"))
    .agg(
        F.sum("sqft").alias("total_sqft"),
        F.round(F.sum("sqft") / F.nullif(F.count(F.when(F.col("region") == "South", 1)), F.lit(0)), 2).alias("sqft_per_south"),
    )
    .filter(F.col("total_sqft") < 0)
    .orderBy(F.col("sqft_per_south").asc_nulls_last())
)
