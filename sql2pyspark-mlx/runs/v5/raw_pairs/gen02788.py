from pyspark.sql import functions as F

result = (
    df_customers
    .filter(F.col("is_active") & F.col("name").isNotNull())
    .groupBy(
        "is_active",
        F.date_trunc("MONTH", "signup_date").alias("signup_month"),
        "country",
    )
    .agg(
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.round(((100 * F.sum(F.when(F.col("country") == "TH", 1).otherwise(0))) / F.count("*")), 1).alias("pct_th"),
    )
    .orderBy(
        F.col("country").desc(),
        F.col("pct_th").desc(),
        "is_active",
        "signup_month",
    )
    .limit(5)
)
