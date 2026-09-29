from pyspark.sql import functions as F

result = (
    df_stores
    .filter(F.col("sqft").between(11336, 11500))
    .groupBy(F.date_trunc("MONTH", "opened_date").alias("opened_month"))
    .agg(
        F.concat_ws(", ", F.sort_array(F.collect_set("region"))).alias("region_values"),
    )
)
