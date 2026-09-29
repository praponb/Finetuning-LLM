from pyspark.sql import functions as F
from pyspark.sql.window import Window

w = Window.partitionBy("account_type").orderBy("opened_date")
w2 = Window.partitionBy("account_type", "customer_id").orderBy(F.col("balance").desc(), "id")

base = (
    df_accounts
    .filter((F.col("opened_date") > F.to_date(F.lit("2023-09-14"))) & F.col("account_type").isin(["CREDIT", "LOAN", "CHECKING"]))
    .select("*", F.initcap(F.trim("account_type")).alias("account_type_title"))
)

deduped = (
    base
    .withColumn("row_num", F.rank().over(w))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

ranked = (
    deduped
    .withColumn("rn", F.row_number().over(w2))
    .filter(F.col("rn") == 1)
)

result = (
    ranked
    .cube("status")
    .agg(
        F.countDistinct("status").alias("distinct_status"),
        F.percentile_approx("balance", [0.25, 0.5, 0.75]).alias("balance_quartiles"),
        F.max(F.when(F.col("account_type") == "CREDIT", F.col("balance"))).alias("credit_max_balance"),
        F.max(F.when(F.col("status") == "OPEN", F.col("balance"))).alias("open_max_balance"),
        F.grouping_id().alias("grouping_level"),
    )
    .orderBy(F.col("open_max_balance").asc_nulls_last())
)
