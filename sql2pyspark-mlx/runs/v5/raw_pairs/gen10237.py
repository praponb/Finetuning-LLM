from pyspark.sql import functions as F

base = (
    df_customers
    .filter(F.col("country").isin(["JP", "UK"]) & F.col("age").between(34, 58) & (F.length("email") >= 10))
)

agg = (
    base
    .groupBy("country")
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.sum("credit_limit").alias("total_credit_limit"),
    )
)

result = (
    agg
    .select("country", "city_list", "total_credit_limit")
    .orderBy(F.col("country").desc())
)
