from pyspark.sql import functions as F

result = (
    df_customers
    .filter(~F.col("is_active") & F.col("name").isNotNull())
    .cube(
        F.when(F.col("credit_limit") >= 13700, "high").when(F.col("credit_limit") >= 2600, "mid").otherwise("low").alias("credit_limit_bucket"),
        F.month("signup_date").alias("signup_month"),
    )
    .agg(
        F.countDistinct("age").alias("unique_countrys"),
        F.max("credit_limit").alias("max_credit_limit"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
    )
    .orderBy(F.col("max_credit_limit").desc())
)
