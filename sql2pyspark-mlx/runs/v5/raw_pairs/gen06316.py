from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("city").orderBy(F.col("signup_date").desc(), "id")

valid = (
    df_customers
    .filter((F.col("credit_limit") <= 14600) & F.col("city").isNotNull())
    .select(
        "id",
        "email",
        "city",
        "signup_date",
        "credit_limit",
        "age",
        F.split(F.lower("email"), "@").getItem(0).alias("email_user"),
        F.coalesce("city", F.lit("N/A")).alias("city_filled"),
    )
)

base = (
    valid
    .filter((F.col("signup_date") > F.lit("2023-09-04").cast("date")) & (F.col("city").eqNullSafe(F.lit("Singapore"))))
    .select("*", F.ceil("credit_limit").alias("credit_limit_whole"))
)

kept = (
    base
    .alias("s")
    .join(df_subscriptions.alias("c"), (F.col("c.customer_id") == F.col("s.id")) & (F.col("c.end_date") < F.col("s.signup_date")), "left_anti")
)

ranked = (
    kept
    .withColumn("row_num", F.row_number().over(w))
    .filter(F.col("row_num") == 1)
)

result = (
    ranked
    .withColumn("credit_limit_tier", F.when(F.col("credit_limit") >= 17200, "PREMIUM").otherwise("STANDARD"))
    .cube("credit_limit_tier")
    .agg(
        F.round(F.sum("credit_limit") / F.nullif(F.count(F.when(F.col("city") == "Singapore", 1)), F.lit(0)), 2).alias("credit_limit_per_singapore"),
        F.size(F.collect_set("city")).alias("num_cities"),
        F.count_if(F.col("city") == "Tokyo").alias("tokyo_events"),
        F.max(F.when(F.col("city") == "New York", F.col("credit_limit"))).alias("new_york_max_credit_limit"),
        F.sort_array(F.collect_set("city")).alias("city_list"),
        F.bit_and("age").alias("age_bit_and"),
        F.grouping("credit_limit_tier").alias("gid"),
    )
    .orderBy(F.col("credit_limit_tier").desc())
)
