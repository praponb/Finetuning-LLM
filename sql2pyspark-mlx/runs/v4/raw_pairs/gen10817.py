from pyspark.sql import functions as F

src = df_accounts.filter(F.col("status") == "OPEN")

agg = (
    src
    .groupBy("status")
    .agg(
        F.countDistinct("balance").alias("unique_account_types"),
        F.sort_array(F.collect_set("status")).alias("status_list"),
    )
)

result = (
    agg
    .select("status", "unique_account_types", "status_list")
    .orderBy(F.col("unique_account_types").desc(), "status")
    .limit(10)
)
