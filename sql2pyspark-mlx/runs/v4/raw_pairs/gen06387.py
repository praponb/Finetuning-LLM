from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("status", "customer_id").orderBy(F.col("opened_date_year").desc())
w2 = Window.partitionBy("account_type").orderBy(F.col("opened_date_year").asc_nulls_last())
w3 = Window.partitionBy("account_type").orderBy(F.col("opened_date_year").asc_nulls_first())

base = (
    df_accounts
    .filter((F.col("opened_date") > F.lit("2023-09-07").cast("date")) & F.col("status").isNotNull())
    .select(
        "id",
        "customer_id",
        "account_type",
        "opened_date",
        "status",
        F.year("opened_date").alias("opened_date_year"),
    )
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
)

metrics = (
    deduped
    .select(
        "*",
        F.dense_rank().over(w2).alias("opened_date_year_dense_rank"),
        F.percent_rank().over(w3).alias("opened_date_year_percent_rank"),
    )
)

valid = (
    metrics
    .filter(F.col("status").isNotNull())
    .withColumn("opened_date_year_dense_rank_rounded", F.round("opened_date_year_dense_rank", 2))
)

result = (
    valid
    .select(
        "id",
        "account_type",
        "opened_date",
        "status",
        "opened_date_year_dense_rank",
        "opened_date_year_percent_rank",
        "opened_date_year_dense_rank_rounded",
        F.when(F.col("account_type").rlike("^(LOAN|CREDIT)$"), "PRIORITY").otherwise("GROUP_A").alias("account_type_zone"),
    )
    .orderBy(F.col("opened_date_year_dense_rank").asc_nulls_first(), "status")
)
