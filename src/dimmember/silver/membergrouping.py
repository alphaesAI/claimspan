import recordlinkage
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    sha2, col, row_number, substring, 
    upper, trim, regexp_replace, expr, to_json, struct, concat_ws
)
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField, StringType, LongType, IntegerType
)

# Matching rules definitions
RuleMBIColumns = ["identifier_beneficiaryID", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RulePMIDColumns = ["identifier_planMemberID", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RuleUPKColumns = ["identifier_uniquepersonkey", "name_given_first_initial", "name_family_initial", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1", 11]
RuleOtherColumns = ["birthDate_formatted", "name_given_first", "name_family", "telecom_phone_home_formatted", "address_permanent_line1", 14]

RulesAll = [RuleMBIColumns, RulePMIDColumns, RuleUPKColumns, RuleOtherColumns]
CompareColumns = [
    "name_family_initial", "name_given_first_initial", "name_family", "name_given_first", "identifier_beneficiaryID", 
    "identifier_planMemberID", "identifier_uniquepersonkey", "birthDate_formatted", "telecom_phone_home_formatted", "address_permanent_line1"
]


def LoadSource(df_bronze):
    """Transforms raw bronze PySpark DataFrame into prepared feature dataset."""
    windowPartition = Window.partitionBy(col("FileID")).orderBy(col("FileID"))

    return df_bronze.withColumn("RecordHash", sha2(to_json(struct("*")), 256)) \
        .distinct() \
        .withColumn("RowNumber", row_number().over(windowPartition)) \
        .withColumn("UniqueRecord", concat_ws("-", col("FileID"), col("RowNumber"))) \
        .withColumn("FileID", col("FileID").cast(LongType())) \
        .withColumn("FileLayoutID", col("FileLayoutID").cast(IntegerType())) \
        .withColumn("name_family", upper(trim(col("name_family")))) \
        .withColumn("name_given_first", upper(trim(col("name_given_first")))) \
        .withColumn("name_family_initial", upper(substring(trim(col("name_family")), 1, 1))) \
        .withColumn("name_given_first_initial", upper(substring(trim(col("name_given_first")), 1, 1))) \
        .withColumn("birthDate", expr("try_cast(birthDate as string)")) \
        .withColumn("birthDate_formatted", expr("regexp_replace(try_cast(birthDate as string), '[^0-9]', '')")) \
        .withColumn("gender", upper(trim(col("gender")))) \
        .withColumn("address_permanent_line1", upper(trim(col("address_permanent_line1")))) \
        .withColumn("telecom_phone_home", trim(col("telecom_phone_home"))) \
        .withColumn("telecom_phone_home_formatted", regexp_replace(trim(col("telecom_phone_home")), "[^0-9]", "")) \
        .withColumn("identifier_planMemberID", upper(trim(col("identifier_planMemberID")))) \
        .withColumn("identifier_beneficiaryID", upper(trim(col("identifier_beneficiaryID")))) \
        .withColumn("identifier_uniquepersonkey", upper(trim(col("identifier_uniquepersonkey")))) \
        .select(
            "UniqueRecord", "FileLayoutID", "FileID", "RowNumber", "name_family", "name_given_first", 
            "gender", "telecom_phone_home", "address_permanent_line1", "birthDate", "birthDate_formatted", 
            "identifier_planMemberID", "identifier_beneficiaryID", "identifier_uniquepersonkey", 
            "name_family_initial", "name_given_first_initial", "telecom_phone_home_formatted"
        )


def RulesToCompare(rules, pandasMem_df):
    """Executes record linkage rules against a partition Pandas DataFrame."""
    matchesAllRules_df = pd.DataFrame()
    for lst in rules:
        indexer = recordlinkage.Index()
        indexer.block(lst[0])
        candidatesBlock = indexer.index(pandasMem_df)
        
        compareBlock = recordlinkage.Compare()
        threshold = lst[-1]
        for col_name in CompareColumns:
            compareBlock.exact(col_name, col_name, label=str(col_name))
            
        features = compareBlock.compute(candidatesBlock, pandasMem_df)
        features[lst[0]] = features[lst[0]].apply(lambda x: x * 10)
        
        matchesRule = features[features[lst[:-1]].sum(axis=1) >= threshold]
        matchesRule_df = matchesRule.index.to_frame()
        
        if len(matchesRule_df) > 0:
            matchesAllRules_df = pd.concat([matchesAllRules_df, matchesRule_df])
            
    return matchesAllRules_df


def run_linking_pandas(pandasMem_df: pd.DataFrame) -> pd.DataFrame:
    """Distributed worker function that runs record linkage per partition."""
    if pandasMem_df.empty:
        pandasMem_df["MatchID"] = ""
        return pandasMem_df.astype(str)

    if len(pandasMem_df) == 1:
        pandasMem_df["MatchID"] = pandasMem_df["UniqueRecord"]
        return pandasMem_df.astype(str)

    if pandasMem_df["UniqueRecord"].duplicated().any():
        pandasMem_df = pandasMem_df.reset_index(drop=True)
        pandasMem_df["UniqueRecord"] = pandasMem_df.index.astype(str) + "_" + pandasMem_df["UniqueRecord"].astype(str)
    
    pandasMem_df = pandasMem_df.set_index("UniqueRecord", drop=False)
    dfCombined = RulesToCompare(RulesAll, pandasMem_df)

    if dfCombined.empty:
        finalPandas_df = pandasMem_df.copy()
        finalPandas_df['MatchID'] = finalPandas_df['UniqueRecord']
        return finalPandas_df.reset_index(drop=True).astype(str)

    dfCombined.columns = [0, 1]
    dfMatchedColumnA = dfCombined.rename(columns={0: "A", 1: "B"})
    dfMatchedColumnB = dfCombined.rename(columns={0: "B", 1: "A"})
    
    dfMatched = pd.concat([dfMatchedColumnA, dfMatchedColumnB], ignore_index=True).drop_duplicates()
    
    matchesAll_df = pd.concat([
        dfMatched[["A", "B"]].rename(columns={"A": "Record", "B": "Match"}),
        dfMatched[["B", "A"]].rename(columns={"B": "Record", "A": "Match"})
    ]).reset_index(drop=True)
    
    matchesSame_df = matchesAll_df[["Record", "Record"]].copy()
    matchesSame_df.columns = ['Record', 'Match']
    
    matchesAll_df = pd.concat([matchesAll_df, matchesSame_df], ignore_index=True).drop_duplicates()
    distinctRow = dfMatched.groupby('A').head(1).drop('B', axis=1)
    
    matchesAll_df = pd.merge(matchesAll_df, distinctRow, how="left", left_on="Record", right_on="A").drop(columns=['A'])
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df[["UniqueRecord"]], left_on="Record", right_index=True).rename(columns={"UniqueRecord_y": "RecordID"})
    matchesAll_df = pd.merge(matchesAll_df, pandasMem_df[["UniqueRecord"]], left_on="Match", right_index=True).rename(columns={"UniqueRecord_y": "MatchID"})
    
    matched_df = matchesAll_df.groupby("RecordID") \
            .agg({"MatchID": lambda x: sorted(list(pd.unique(x)))}) \
            .reset_index()
    
    # Mirror your notebook logic: stringify arrays like "ID1,ID2" for SQL SUBSTR processing downstream
    matched_df["MatchID"] = matched_df["MatchID"].apply(lambda x: ",".join(map(str, x)))
    
    matchesAllModified = matchesAll_df.groupby("RecordID").head(1)
    drop_cols = ['MatchID', 'Match', 'Record', 'index', 'UniqueRecord_x']
    matchesAllModified = matchesAllModified.drop(columns=[c for c in drop_cols if c in matchesAllModified.columns])
        
    newDFToMatch = pd.merge(matched_df, matchesAllModified, how="left", on="RecordID")
    finalPandas_df = pandasMem_df.reset_index(drop=True).merge(newDFToMatch, left_on="UniqueRecord", right_on="RecordID", how="left")
    finalPandas_df['MatchID'] = finalPandas_df['MatchID'].fillna(finalPandas_df['UniqueRecord'])
    
    cols_order = list(pandasMem_df.columns) + ["MatchID"]
    return finalPandas_df[cols_order].astype(str)


def ProcessMemberBridge(df_bronze, spark_session=None):
    """Main DLT entrypoint for building the Silver Member Person Bridge dataset."""
    spark = spark_session or SparkSession.getActiveSession()
    
    # 1. Prepare raw inputs into feature dataframe
    sparkMem_df = LoadSource(df_bronze)
    
    # 2. Schema definition for applyInPandas execution
    output_schema = StructType([StructField(c, StringType(), True) for c in sparkMem_df.columns] + [StructField("MatchID", StringType(), True)])
    
    # 3. Distributed linkage execution per partition (grouped by initial to scale across workers)
    linked_df = sparkMem_df.groupBy("name_family_initial").applyInPandas(
        lambda pdf: run_linking_pandas(pdf), 
        schema=output_schema
    )

    # 4. Pure PySpark transformation replicating notebook finalSQL
    match_clean_expr = expr("CASE WHEN INSTR(MatchID, ',') = 0 THEN MatchID ELSE SUBSTR(MatchID, 1, INSTR(MatchID, ',') - 1) END")
    
    w_first = Window.partitionBy(match_clean_expr).orderBy(col("FileID").cast("long").asc(), col("RowNumber").cast("long").asc())
    w_curr = Window.partitionBy(match_clean_expr).orderBy(col("FileID").cast("long").desc(), col("RowNumber").cast("long").desc())

    person_with_ids = linked_df.withColumn("MatchID_Clean", match_clean_expr) \
        .withColumn("FirstPersonIdentifier", row_number().over(w_first)) \
        .withColumn("CurrentPersonIdentifier", row_number().over(w_curr))

    fp = person_with_ids.filter(col("FirstPersonIdentifier") == 1).select(
        col("MatchID_Clean").alias("fp_MatchID"),
        col("UniqueRecord").alias("ESAIInternalPersonID")
    )
    
    bridge = person_with_ids.join(fp, person_with_ids.MatchID_Clean == fp.fp_MatchID, "left") \
        .withColumn("IsCurrent", expr("CASE WHEN CurrentPersonIdentifier = 1 THEN 1 ELSE 0 END"))

    w_pmid = Window.partitionBy("identifier_planMemberID").orderBy(col("FileID").cast("long").desc(), col("RowNumber").cast("long").desc())
    w_upk = Window.partitionBy("identifier_uniquepersonkey").orderBy(col("FileID").cast("long").desc(), col("RowNumber").cast("long").desc())

    bridge_curr = bridge.withColumn(
        "PlanMemberIdModified", expr("IFNULL(NULLIF(identifier_planMemberID, 'None'), '')")
    ).withColumn(
        "UniquePersonKeyModified", expr("IFNULL(NULLIF(identifier_uniquepersonkey, 'None'), '')")
    ).withColumn(
        "IsCurrentPlanMemberID", expr("CASE WHEN IFNULL(identifier_planMemberID, 'None') = 'None' THEN NULL WHEN ROW_NUMBER() OVER (PARTITION BY identifier_planMemberID ORDER BY COALESCE(CAST(FileID AS LONG), 0) DESC, COALESCE(CAST(RowNumber AS LONG), 0) DESC) = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "IsCurrentUniquePersonKey", expr("CASE WHEN IFNULL(identifier_uniquepersonkey, 'None') = 'None' THEN NULL WHEN ROW_NUMBER() OVER (PARTITION BY identifier_uniquepersonkey ORDER BY COALESCE(CAST(FileID AS LONG), 0) DESC, COALESCE(CAST(RowNumber AS LONG), 0) DESC) = 1 THEN 1 ELSE 0 END")
    ).withColumn(
        "IsOriginalMemberID", expr("CASE WHEN ESAIInternalPersonID = UniqueRecord THEN 1 ELSE 0 END")
    ).withColumn(
        "PMUP", concat_ws("-", col("PlanMemberIdModified"), col("UniquePersonKeyModified"))
    ).withColumn(
        "IsCurrentPMUPComputed", expr("CAST(COALESCE(IsCurrentPlanMemberID, IsCurrentUniquePersonKey, 1) AS INT)")
    )

    # 5. Output finalized PySpark DataFrame matching target Silver schema
    return bridge_curr.select(
        "ESAIInternalPersonID",
        "IsCurrent",
        "UniqueRecord",
        col("FileLayoutID").cast("int").alias("FileLayoutID"),
        col("FileID").cast("long").alias("FileID"),
        "name_family",
        "name_given_first",
        "birthDate",
        "gender",
        "address_permanent_line1",
        "telecom_phone_home",
        "identifier_planMemberID",
        "identifier_beneficiaryID",
        "identifier_uniquepersonkey",
        sha2(concat_ws('|',
            expr("IFNULL(ESAIInternalPersonID,'')"), expr("IFNULL(CAST(IsCurrent AS STRING),'')"), expr("IFNULL(UniqueRecord,'')"),
            expr("IFNULL(CAST(FileLayoutID AS STRING),'')"), expr("IFNULL(CAST(FileID AS STRING),'')"), expr("IFNULL(name_family,'')"),
            expr("IFNULL(name_given_first,'')"), expr("IFNULL(birthDate,'')"), expr("IFNULL(gender,'')"), expr("IFNULL(address_permanent_line1,'')"),
            expr("IFNULL(telecom_phone_home,'')"), expr("IFNULL(identifier_planMemberID,'')"), expr("IFNULL(identifier_beneficiaryID,'')"), expr("IFNULL(identifier_uniquepersonkey,'')"),
            expr("IFNULL(CAST(IsCurrentPlanMemberID AS STRING),'')"), expr("IFNULL(CAST(IsCurrentUniquePersonKey AS STRING),'')"),
            expr("IFNULL(CAST(IsOriginalMemberID AS STRING),'')"), expr("IFNULL(PMUP,'')"), expr("IFNULL(CAST(IsCurrentPMUPComputed AS STRING),'')")
        ), 256).alias("hashKey"),
        "IsCurrentPlanMemberID",
        "IsCurrentUniquePersonKey",
        "IsOriginalMemberID",
        "PMUP",
        col("IsCurrentPMUPComputed").alias("IsCurrentPMUP")
    )