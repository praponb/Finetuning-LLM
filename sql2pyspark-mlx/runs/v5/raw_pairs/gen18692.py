from pyspark.sql import functions as F

result = (
    df_shipments
    .filter(F.col("order_id").isin([4, 5]) & (F.col("order_id").isNull() | F.col("cost").between(71, 93)))
    .groupBy(F.year("delivered_date").alias("delivered_year"))
    .agg(
        F.round(((100 * F.sum(F.when(F.col("status") == "IN_TRANSIT", 1).otherwise(0))) / F.count("*")), 1).alias("pct_in_transit"),
    )
    .filter(F.col("pct_in_transit") < 53)
    .orderBy(F.col("pct_in_transit").desc())
)
