class DashBoardBulkQuriesOfTable:

    query_string_daily_dashboard = """
                        SELECT * FROM 
                        (
                        SELECT  TO_DATE(A.RECORD_DATE) RECORD_DATE,
                            CASE WHEN SOURCE_SYSTEM_FLAG = 'D2R_PR' THEN 'PHARMRETAIL'
                                WHEN A.SOURCE_SYSTEM_FLAG='MTD' THEN 'PHARMANALYTICS MTD'
                                WHEN A.SOURCE_SYSTEM_FLAG='MONTHLY' THEN 'PHARMANALYTICS MONTHLY'
                                ELSE A.SOURCE_SYSTEM_FLAG END PROJECT,
                            COUNT(1) AS "TOTAL DELTA",
                            COUNT(CASE WHEN (MAPPING_FLAG LIKE 'PR%' OR MAPPING_FLAG LIKE '%REGEX%') AND MAPPING_FLAG NOT LIKE '%BATCH-MATCH%' THEN 1 END) AS "REGEX-MATCH",
                            COUNT(CASE WHEN MAPPING_FLAG LIKE '%BATCH-MATCH%' THEN 1 END) AS "BATCH-MATCH",
                            COUNT(CASE WHEN MAPPING_FLAG = 'AUTOMAP' THEN 1 END) AS "INFORMATICA-MATCH",
                            "REGEX-MATCH"+"BATCH-MATCH"+"INFORMATICA-MATCH" AS "TOTAL AUTOMAPPED",
                            ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
                            COUNT(CASE WHEN MAPPING_FLAG IN ('MANUAL-MATCH' ) THEN 1 END) AS "SENT FOR MANUAL"
                        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                        LEFT JOIN 
                            PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                        WHERE  TO_DATE(A.RECORD_DATE) = :daterange 
                        GROUP BY ALL

                        UNION ALL

                        SELECT TO_DATE(RECORDDATE) AS RECORD_DATE,
                        'PHARMCONNECT-LIVE' PROJECT,
                        COUNT(1) "TOTAL DELTA",
                        NULL AS "REGEX-MATCH",
                        NULL AS "BATCH-MATCH",
                        NULL AS "INFORMATICA-MATCH",
                        COUNT(CASE WHEN APP_STATUS = 'CMP' THEN 1 END) AS "TOTAL AUTOMAPPED",
                        ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
                        COUNT(CASE WHEN APP_STATUS = 'MANR' THEN 1 END) AS "SENT FOR MANUAL"
                        FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING
                        WHERE  TO_DATE(RECORDDATE)  = :daterange
                        GROUP BY TO_DATE(RECORDDATE)

                        UNION ALL

                        SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                            'PHARMCONNECT-BATCH' PROJECT,
                            COUNT(1) AS "TOTAL DELTA",
                            COUNT(CASE WHEN MAPPING_FLAG LIKE '%REGEX-MATCH' OR  MAPPING_FLAG LIKE '%NEW-MATCH' THEN 1 END) AS "REGEX-MATCH",
                            NULL AS "BATCH-MATCH",
                            NULL AS "INFORMATICA-MATCH",
                            "REGEX-MATCH" AS "TOTAL AUTOMAPPED",
                            ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
                            ("TOTAL DELTA"-"TOTAL AUTOMAPPED") AS "SENT FOR MANUAL"
                        FROM PHARMMDM.MAPPING.AUTOMAPPING_ADHOC_INPUT A
                        LEFT JOIN PHARMMDM.MAPPING.AUTOMAPPING_ADHOC_REGEXBATCH_MATCH B
                            ON A.PK = B.PK
                            AND A.SOURCE_SYSTEM_FLAG = B.SOURCE_SYSTEM_FLAG
                        WHERE TO_DATE(A.RECORD_DATE) = :daterange
                        GROUP BY ALL
                        )
                        ORDER BY TO_DATE(RECORD_DATE) DESC, PROJECT;
            """
    
    query_string_manual_mapping = """
                                    SELECT
                                    RECORD_DATE,
                                    SOURCE_SYSTEM,
                                    "TOTAL MANUAL EFFORTS",
                                    "MAPPED TO MDM CODE",
                                    "PUSH TO OTHERS"
                                    FROM 
                                    (
                                        SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                                        'PHARMRETAIL' AS SOURCE_SYSTEM,
                                        --MAPPING_FLAG,
                                        COUNT(1) AS "TOTAL MANUAL EFFORTS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                                        FROM  PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT A
                                        WHERE MAPPING_FLAG IN ('MAPPING-UPLOAD','MAPPING-SCREEN')
                                        AND SOURCE_SYSTEM='pharmretail'
                                        AND TO_DATE(RECORD_DATE) = :daterange
                                        GROUP BY ALL
                                        
                                        UNION ALL
                                        
                                        SELECT 
                                            TO_DATE(A.RECORD_DATE) AS RECORD_DATE, 
                                            CASE WHEN B.SOURCE_SYSTEM_FLAG='MONTHLY' THEN 'PHARMANALYTICS MONTHLY'
                                            WHEN B.SOURCE_SYSTEM_FLAG='MTD' THEN 'PHARMANALYTICS MTD' END SOURCE_SYSTEM,
                                            COUNT(1) AS "TOTAL MANUAL EFFORTS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                                        FROM 
                                        (
                                            -- Combining both the manual mapping and upload data with better condition management
                                            SELECT 
                                                DISTINCT DISTRIBUTOR_CODE, REGEX,MDM_PRODUCT_CODE, RECORD_DATE
                                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
                                            WHERE SOURCE_SYSTEM IN ('pt-batch', 'ts-batch', 'pharmanalytics') AND TO_DATE(RECORD_DATE) = :daterange
                                            UNION
                                            SELECT 
                                                DISTINCT DISTRIBUTOR_CODE, 
                                                COALESCE(REGEX, UPPER(PHARMANALYTICS.stage.FN_Searchcolumn_final(CASE WHEN LENGTH(PACK) = 1 AND TRY_TO_DOUBLE(REGEXP_REPLACE(PACK, '[^.0-9]+')) = 0 THEN PRODUCT_NAME ELSE CONCAT(PRODUCT_NAME, COALESCE(PACK, '')) END))) AS REGEX,MDM_PRODUCT_CODE,
                                                RECORD_DATE
                                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
                                            WHERE SOURCE_SYSTEM IN ('pt-batch', 'ts-batch', 'pharmanalytics') AND TO_DATE(RECORD_DATE) = :daterange
                                        ) A
                                        INNER JOIN 
                                        (
                                            SELECT 
                                                DISTRIBUTOR_CODE, REGEX, SOURCE_SYSTEM_FLAG
                                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING
                                            WHERE SOURCE_SYSTEM_FLAG IN ('MONTHLY', 'MTD')
                                        ) B ON A.DISTRIBUTOR_CODE = B.DISTRIBUTOR_CODE AND A.REGEX = B.REGEX
                                        GROUP BY ALL

                                        UNION ALL
                                        
                                        SELECT TO_DATE(A.RECORDDATE) AS RECORD_DATE,
                                        'PHARMCONNECT-LIVE' AS SOURCE_SYSTEM,
                                        COUNT(1) AS "TOTAL MANUAL EFFORTS",
                                        COUNT(CASE WHEN B.MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                                        COUNT(CASE WHEN B.MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                                        FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING A
                                        LEFT JOIN PHARMMDM.MASTERDATA.MDM_API_PRODUCT_MAPPING_RESPONSE B ON A.SRC_UUIDKEY=B.UUIDKEY AND A.SRC_SOURCE_ENTITY_CODE=B.SOURCEENTITYCODE AND A.SRC_PRODUCT_NAME=B.PRODUCTNAME 
                                        WHERE A.APP_STATUS = 'MANR'
                                        AND  TO_DATE(A.RECORDDATE) = :daterange
                                        GROUP BY ALL

                                        UNION ALL
                                        
                                        SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                                        'PHARMCONNECT-BATCH' AS SOURCE_SYSTEM,
                                        --MAPPING_FLAG,
                                        COUNT(1) AS "TOTAL MANUAL EFFORTS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                                        COUNT(CASE WHEN MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                                        FROM  PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT A
                                        WHERE MAPPING_FLAG IN ('MAPPING-UPLOAD','MAPPING-SCREEN')
                                        AND SOURCE_SYSTEM='pharmconnect'
                                        AND TO_DATE(RECORD_DATE) = :daterange
                                        GROUP BY ALL
                                    )
                                    ORDER BY RECORD_DATE DESC, SOURCE_SYSTEM;
            """
        
    query_string_active_user = """
        SELECT RECORD_DATE, COUNT(DISTINCT CREATED_BY) "ACTIVE USERS"
        FROM
        (
        SELECT 
            DISTINCT CREATED_BY,
            TO_DATE(RECORD_DATE) RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
        WHERE TO_DATE(RECORD_DATE) = :daterange
        UNION
        SELECT 
            DISTINCT CREATED_BY,
            TO_DATE(RECORD_DATE) RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
        WHERE TO_DATE(RECORD_DATE) = :daterange
        )
        GROUP BY ALL
        ORDER BY RECORD_DATE DESC;
        """

    query_string_avg_active_user = """
            SELECT ROUND(AVG("ACTIVE USERS")) AS "ACTIVE USERS"
            FROM
            (
            SELECT RECORD_DATE, COUNT(DISTINCT CREATED_BY) "ACTIVE USERS"
            FROM
            (
            SELECT 
                DISTINCT CREATED_BY,
                TO_DATE(RECORD_DATE) RECORD_DATE
            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
            WHERE TO_DATE(RECORD_DATE) = :daterange
            UNION
            SELECT 
                DISTINCT CREATED_BY,
                TO_DATE(RECORD_DATE) RECORD_DATE
            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
            WHERE TO_DATE(RECORD_DATE) = :daterange
            )
            GROUP BY ALL
            );
        """

    query_string_performer_for_selected_range = """
        SELECT INITCAP(REPLACE(CREATED_BY,'.',' ')) "USER", COUNT(1) MANUAL_EFFORTS
        FROM
        (
        SELECT 
            CREATED_BY,
            TO_DATE(RECORD_DATE) RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
        WHERE TO_DATE(RECORD_DATE) = :daterange
        UNION ALL
        SELECT 
            CREATED_BY,
            TO_DATE(RECORD_DATE) RECORD_DATE
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
        WHERE TO_DATE(RECORD_DATE) = :daterange 
        )
        GROUP BY ALL
        ORDER BY COUNT(1) DESC
        LIMIT 5;
            """
    
    query_string_manufacturer_mapping = """
        SELECT 
        TO_DATE(RECORD_DATE) AS RECORD_DATE, 
        CASE WHEN SOURCE_SYSTEM = 'pharmretail' THEN 'PHARMRETAIL'
            WHEN SOURCE_SYSTEM='pharmanalytics-mtd' THEN 'PHARMANALYTICS MTD'
            WHEN SOURCE_SYSTEM='pharmanalytics-monthly' THEN 'PHARMANALYTICS MONTHLY'
            ELSE SOURCE_SYSTEM END PROJECT,
            -- MODIFIED_BY, 
            COUNT(*) AS "MANUFACTURER MAPPING COUNT"
        FROM PHARMMDM.MASTERDATA.MDM_DIST2COMP_MAPPING_RESPONSE
        WHERE TO_DATE(RECORD_DATE) =:daterange   
            AND MODIFIED_BY IS NOT NULL
        GROUP BY ALL
        ORDER BY TO_DATE(RECORD_DATE) DESC,PROJECT ;
        """
    
    query_string_overall_project_wise_automapping = """
        SELECT * FROM 
        (
        SELECT -- TO_DATE(A.RECORD_DATE) RECORD_DATE,
            CASE WHEN SOURCE_SYSTEM_FLAG = 'D2R_PR' THEN 'PHARMRETAIL'
                WHEN A.SOURCE_SYSTEM_FLAG='MTD' THEN 'PHARMANALYTICS MTD'
                WHEN A.SOURCE_SYSTEM_FLAG='MONTHLY' THEN 'PHARMANALYTICS MONTHLY'
                ELSE A.SOURCE_SYSTEM_FLAG END PROJECT,
            COUNT(1) AS "TOTAL DELTA",
            COUNT(CASE WHEN (MAPPING_FLAG LIKE 'PR%' OR MAPPING_FLAG LIKE '%REGEX%') AND MAPPING_FLAG NOT LIKE '%BATCH-MATCH%' THEN 1 END) AS "REGEX-MATCH",
            COUNT(CASE WHEN MAPPING_FLAG LIKE '%BATCH-MATCH%' THEN 1 END) AS "BATCH-MATCH",
            COUNT(CASE WHEN MAPPING_FLAG = 'AUTOMAP' THEN 1 END) AS "INFORMATICA-MATCH",
            "REGEX-MATCH"+"BATCH-MATCH"+"INFORMATICA-MATCH" AS "TOTAL AUTOMAPPED",
            ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
            COUNT(CASE WHEN MAPPING_FLAG IN ('MANUAL-MATCH' ) THEN 1 END) AS "SENT FOR MANUAL"
        FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
        LEFT JOIN 
            PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
        WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
        GROUP BY ALL

        UNION ALL

        SELECT --TO_DATE(RECORDDATE) AS RECORD_DATE,
        'PHARMCONNECT-LIVE' PROJECT,
        COUNT(1) "TOTAL DELTA",
        NULL AS "REGEX-MATCH",
        NULL AS "BATCH-MATCH",
        NULL AS "INFORMATICA-MATCH",
        COUNT(CASE WHEN APP_STATUS = 'CMP' THEN 1 END) AS "TOTAL AUTOMAPPED",
        ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
        COUNT(CASE WHEN APP_STATUS = 'MANR' THEN 1 END) AS "SENT FOR MANUAL"
        FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING
        WHERE  TO_DATE(RECORDDATE) =:daterange
        --GROUP BY TO_DATE(RECORDDATE)

        UNION ALL

        SELECT --TO_DATE(A.RECORD_DATE) RECORD_DATE,
            'PHARMCONNECT-BATCH' PROJECT,
            COUNT(1) AS "TOTAL DELTA",
            COUNT(CASE WHEN MAPPING_FLAG LIKE '%REGEX-MATCH' OR  MAPPING_FLAG LIKE '%NEW-MATCH' THEN 1 END) AS "REGEX-MATCH",
            NULL AS "BATCH-MATCH",
            NULL AS "INFORMATICA-MATCH",
            "REGEX-MATCH" AS "TOTAL AUTOMAPPED",
            ROUND(("TOTAL AUTOMAPPED"/"TOTAL DELTA")*100) AS "AUTOMAPPED %",
            ("TOTAL DELTA"-"TOTAL AUTOMAPPED") AS "SENT FOR MANUAL"
        FROM PHARMMDM.MAPPING.AUTOMAPPING_ADHOC_INPUT A
        LEFT JOIN PHARMMDM.MAPPING.AUTOMAPPING_ADHOC_REGEXBATCH_MATCH B
            ON A.PK = B.PK
            AND A.SOURCE_SYSTEM_FLAG = B.SOURCE_SYSTEM_FLAG
        WHERE TO_DATE(A.RECORD_DATE)=:daterange
        GROUP BY ALL
        )
        ORDER BY PROJECT ;
    """

class DashBoardBulkQuriesOfCharts:

    query_string_weekly_manual_efforts = """
                        SELECT
                        RECORD_DATE,
                        SOURCE_SYSTEM,
                        "TOTAL MANUAL EFFORTS",
                        "MAPPED TO MDM CODE",
                        "PUSH TO OTHERS"
                        FROM (
                        SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                        'PHARMRETAIL' AS SOURCE_SYSTEM,
                        --MAPPING_FLAG,
                        COUNT(1) AS "TOTAL MANUAL EFFORTS",
                        COUNT(CASE WHEN MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                        COUNT(CASE WHEN MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                        FROM  PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT A
                        WHERE MAPPING_FLAG IN ('MAPPING-UPLOAD','MAPPING-SCREEN')
                        AND SOURCE_SYSTEM='pharmretail'
                        AND TO_DATE(RECORD_DATE) = :daterange
                        GROUP BY ALL

                        UNION ALL

                        SELECT 
                            TO_DATE(A.RECORD_DATE) AS RECORD_DATE, 
                            CASE WHEN B.SOURCE_SYSTEM_FLAG='MONTHLY' THEN 'PHARMANALYTICS MONTHLY'
                            WHEN B.SOURCE_SYSTEM_FLAG='MTD' THEN 'PHARMANALYTICS MTD' END SOURCE_SYSTEM,
                            COUNT(1) AS "TOTAL MANUAL EFFORTS",
                        COUNT(CASE WHEN MDM_PRODUCT_CODE NOT LIKE 'MPC%' THEN 1 END) AS "PUSH TO OTHERS",
                        COUNT(CASE WHEN MDM_PRODUCT_CODE LIKE 'MPC%' THEN 1 END) AS "MAPPED TO MDM CODE"
                        FROM 
                        (
                            -- Combining both the manual mapping and upload data with better condition management
                            SELECT 
                                DISTINCT DISTRIBUTOR_CODE, REGEX,MDM_PRODUCT_CODE, RECORD_DATE
                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_MAPPING
                            WHERE SOURCE_SYSTEM IN ('pt-batch', 'ts-batch', 'pharmanalytics') AND TO_DATE(RECORD_DATE) = :daterange
                            UNION
                            SELECT 
                                DISTINCT DISTRIBUTOR_CODE, 
                                COALESCE(REGEX, UPPER(PHARMANALYTICS.stage.FN_Searchcolumn_final(CASE WHEN LENGTH(PACK) = 1 AND TRY_TO_DOUBLE(REGEXP_REPLACE(PACK, '[^.0-9]+')) = 0 THEN PRODUCT_NAME ELSE CONCAT(PRODUCT_NAME, COALESCE(PACK, '')) END))) AS REGEX,MDM_PRODUCT_CODE,
                                RECORD_DATE
                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MANUAL_UPLOAD
                            WHERE SOURCE_SYSTEM IN ('pt-batch', 'ts-batch', 'pharmanalytics') AND TO_DATE(RECORD_DATE) = :daterange
                        ) A
                        INNER JOIN 
                        (
                            SELECT 
                                DISTRIBUTOR_CODE, REGEX, SOURCE_SYSTEM_FLAG
                            FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING
                            WHERE SOURCE_SYSTEM_FLAG IN ('MONTHLY', 'MTD')
                        ) B ON A.DISTRIBUTOR_CODE = B.DISTRIBUTOR_CODE AND A.REGEX = B.REGEX
                        GROUP BY ALL
                        )
                        ORDER BY RECORD_DATE DESC, SOURCE_SYSTEM;
                """
    
    query_string_product_addition = """
                SELECT TO_DATE(CREATED_AT) RECORD_DATE,SOURCETYPE1, COUNT(1) "PRODUCT ADDITION" FROM PHARMMDM.MASTERDATA.MDM_PRODUCT_MASTER 
                WHERE TO_DATE(CREATED_AT)=:daterange
                GROUP BY ALL
                ORDER BY  TO_DATE(CREATED_AT) DESC;
    """

    query_string_pharmretail = """
                    SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'TOTAL AUTOMAPPED' AS PARAMETER,
                    COUNT(CASE WHEN MAPPING_FLAG LIKE 'PR%' OR MAPPING_FLAG LIKE '%REGEX%' OR MAPPING_FLAG LIKE '%BATCH-MATCH%' OR MAPPING_FLAG = 'AUTOMAP' THEN 1 END) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG = 'D2R_PR'
                GROUP BY ALL 
                UNION ALL

                SELECT  TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'TOTAL DELTA' AS PARAMETER,
                    COUNT(1) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG = 'D2R_PR'
                GROUP BY ALL   
                UNION ALL

                SELECT  TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'SENT TO MANUAL' AS PARAMETER,
                    COUNT(CASE WHEN MAPPING_FLAG IN ('MANUAL-MATCH' ) THEN 1 END) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG = 'D2R_PR'
                GROUP BY ALL 
                ;
    """

    query_string_pharmanalytics = """
                SELECT * FROM 
                (
                SELECT  TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'TOTAL DELTA' AS PARAMETER,
                    COUNT(1) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG IN ('MONTHLY','MTD')
                GROUP BY ALL   

                UNION ALL

                SELECT TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'TOTAL AUTOMAPPED' AS PARAMETER,
                    COUNT(CASE WHEN MAPPING_FLAG LIKE 'PR%' OR MAPPING_FLAG LIKE '%REGEX%' OR MAPPING_FLAG LIKE '%BATCH-MATCH%' OR MAPPING_FLAG = 'AUTOMAP' THEN 1 END) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG IN ('MONTHLY','MTD')
                GROUP BY ALL 

                UNION ALL

                SELECT  TO_DATE(A.RECORD_DATE) RECORD_DATE,
                    'SENT TO MANUAL' AS PARAMETER,
                    COUNT(CASE WHEN MAPPING_FLAG IN ('MANUAL-MATCH' ) THEN 1 END) AS "RECORDS"
                FROM PHARMMDM.MASTERDATA.MDM_DIST2PROD_MAPPING_LANDING A
                LEFT JOIN 
                    PHARMMDM.MASTERDATA.MDM_DIST2PROD_BATCH_AUDIT C ON A.PK = C.PK
                WHERE  TO_DATE(A.RECORD_DATE)=:daterange --BETWEEN DATEADD(DAY, -30, CURRENT_DATE) AND DATEADD(DAY, 0, CURRENT_DATE)
                AND SOURCE_SYSTEM_FLAG IN ('MONTHLY','MTD')
                GROUP BY ALL 
                )
                ORDER BY RECORD_DATE DESC, PARAMETER ;
    """

    query_string_pharmconnect = """
                SELECT TO_DATE(RECORDDATE) RECORD_DATE,
                'TOTAL AUTOMAPPED' AS PARAMETER,
                COUNT(CASE WHEN APP_STATUS = 'CMP' THEN 1 END) AS "RECORDS"
            FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING
            WHERE  TO_DATE(RECORDDATE) =:daterange
            GROUP BY TO_DATE(RECORDDATE) 

            UNION ALL

            SELECT  TO_DATE(RECORDDATE) RECORD_DATE,
                'SENT TO MANUAL' AS PARAMETER,
                COUNT(CASE WHEN APP_STATUS = 'MANR' THEN 1 END) AS "RECORDS"
            FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING
            WHERE  TO_DATE(RECORDDATE) =:daterange
            GROUP BY TO_DATE(RECORDDATE) 

            UNION ALL

            SELECT  TO_DATE(RECORDDATE) RECORD_DATE,
                'TOTAL DELTA' AS PARAMETER,
                COUNT(1) AS "RECORDS"
            FROM PHARMMDM.MASTERDATA.MDM_CDI_CDQ_PRODUCT_MAPPING
            WHERE  TO_DATE(RECORDDATE) =:daterange
            GROUP BY TO_DATE(RECORDDATE) 
            ;
    """