from pyspark.sql import functions as F

dim_totals = (
    df_accounts
    .alias("a")
    .join(df_customers.alias("d"), F.col("a.customer_id") == F.col("d.id"), "left")
    .rollup(F.col("d.city").alias("city"), F.col("a.status").alias("status"))
    .agg(
        F.count("*").alias("rows_total"),
        F.min(F.col("a.balance")).alias("min_balance"),
    )
)

result = (
    dim_totals
    .select(
        "city",
        "status",
        "rows_total",
        "min_balance",
        F.when(F.col("city").rlike("^(Singapore|Berlin)$"), "EXTENDED").when(F.col("city").rlike("^(Bangkok|Chiang Mai)$"), "CORE").otherwise("GROUP_A").alias("city_zone"),
    )
    .orderBy(F.col("rows_total").desc())
)
