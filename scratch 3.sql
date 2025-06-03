-- CTE: Collect transactions marked as 'collect'
WITH
    COLLECT AS (
        SELECT DISTINCT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            1 AS IS_COLLECT
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%collect%'
    ),
    -- CTE: Main data preparation
    MAIN AS (
        SELECT
            T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            COALESCE(T2.IS_COLLECT, 0) IS_COLLECT,
            T1.CARDUP_PAYMENT_CARD_TYPE,
            DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS) AS MONTH_TRANSACTED,
            MIN(DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS)) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS FIRST_TX_MONTH,
            MAX(DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS)) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS LAST_TX_MONTH,
            COUNT(DISTINCT T1.DWH_CARDUP_PAYMENT_ID) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS PAYMENT_COUNT,
            COUNT(DISTINCT T1.CARDUP_PAYMENT_PAYMENT_TYPE) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS PAYMENT_TYPE_COUNT,
            T1.CARDUP_PAYMENT_USD_AMT AS USD_AMT,
            T1.CARDUP_PAYMENT_NET_REVENUE_USD_AMT AS NET_REVENUE,
            T1.CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT AS TOTAL_REVENUE
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            left outer JOIN COLLECT T2 ON T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            AND CARDUP_PAYMENT_CARD_TYPE IN ('Visa', 'Mastercard')
            AND CARDUP_PAYMENT_PAYEE_BANK_COUNTRY = 'SG'
    ),
    -- Final aggregation with manual pivot
    AGG AS (
        SELECT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            IS_COLLECT,
            MIN(FIRST_TX_MONTH) AS FIRST_TX_MONTH,
            MAX(LAST_TX_MONTH) AS LAST_TX_MONTH,
            MAX(PAYMENT_TYPE_COUNT) AS PAYMENT_TYPE_COUNT,
            -- Total values (all card types)
            SUM(USD_AMT) AS TOTAL_GTV,
            SUM(NET_REVENUE) AS TOTAL_NET_REVENUE,
            SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
            ROUND(SUM(TOTAL_REVENUE) / NULLIF(SUM(USD_AMT), 0), 4) AS CARDUP_FEE,
            ROUND(SUM(NET_REVENUE) / NULLIF(SUM(USD_AMT), 0), 4) AS TAKE_RATE,
            -- Visa-specific
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                    ELSE 0
                END
            ) AS GTV_VISA,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN NET_REVENUE
                    ELSE 0
                END
            ) AS NET_REV_VISA,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN TOTAL_REVENUE
                    ELSE 0
                END
            ) AS REV_VISA,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN TOTAL_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS CARDUP_FEE_VISA,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN NET_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS TAKE_RATE_VISA,
            -- Mastercard-specific
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                    ELSE 0
                END
            ) AS GTV_MC,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN NET_REVENUE
                    ELSE 0
                END
            ) AS NET_REV_MC,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN TOTAL_REVENUE
                    ELSE 0
                END
            ) AS REV_MC,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN TOTAL_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS CARDUP_FEE_MC,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN NET_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS TAKE_RATE_MC
        FROM
            MAIN
        GROUP BY
            1,
            2
    )
SELECT
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
    IS_COLLECT,
    DATEDIFF(MONTH, FIRST_TX_MONTH, CURRENT_DATE) AS TENURE,
    DATEDIFF(MONTH, LAST_TX_MONTH, CURRENT_DATE) AS RECENCY,
    PAYMENT_TYPE_COUNT,
    -- Overall
    TOTAL_GTV,
    TOTAL_REVENUE,
    TOTAL_NET_REVENUE,
    CARDUP_FEE,
    TAKE_RATE,
    -- Visa
    GTV_VISA,
    REV_VISA,
    NET_REV_VISA,
    CARDUP_FEE_VISA,
    TAKE_RATE_VISA,
    -- Mastercard
    GTV_MC,
    REV_MC,
    NET_REV_MC,
    CARDUP_FEE_MC,
    TAKE_RATE_MC
FROM
    AGG;

-- CTE: Collect transactions marked as 'collect'
WITH
    COLLECT AS (
        SELECT DISTINCT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            1 AS IS_COLLECT
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business', 'guest')
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%collect%'
    ),
    -- CTE: Main data preparation
    MAIN AS (
        SELECT
            T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            COALESCE(T2.IS_COLLECT, 0) IS_COLLECT,
            T1.CARDUP_PAYMENT_CARD_TYPE,
            DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS) AS MONTH_TRANSACTED,
            MIN(DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS)) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS FIRST_TX_MONTH,
            MAX(DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS)) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS LAST_TX_MONTH,
            COUNT(DISTINCT T1.DWH_CARDUP_PAYMENT_ID) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS PAYMENT_COUNT,
            COUNT(DISTINCT T1.CARDUP_PAYMENT_PAYMENT_TYPE) OVER (
                PARTITION BY
                    T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            ) AS PAYMENT_TYPE_COUNT,
            T1.CARDUP_PAYMENT_USD_AMT AS USD_AMT,
            T1.CARDUP_PAYMENT_NET_REVENUE_USD_AMT AS NET_REVENUE,
            T1.CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT AS TOTAL_REVENUE
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            left outer JOIN COLLECT T2 ON T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T2.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            AND CARDUP_PAYMENT_CARD_TYPE IN ('Visa', 'Mastercard')
            AND CARDUP_PAYMENT_PAYEE_BANK_COUNTRY = 'SG'
            AND CARDUP_PAYMENT_PROMO_CODE_TAG is not null
            AND CARDUP_PAYMENT_PROMO_CODE_TAG != ''
            AND LOWER(CARDUP_PAYMENT_PROMO_CODE_TAG) != 'null'
            and DATE(DATE_TRUNC('month', T1.CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-04-01')
    ),
    -- Final aggregation with manual pivot
    AGG AS (
        SELECT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            IS_COLLECT,
            MIN(FIRST_TX_MONTH) AS FIRST_TX_MONTH,
            MAX(LAST_TX_MONTH) AS LAST_TX_MONTH,
            MAX(PAYMENT_TYPE_COUNT) AS PAYMENT_TYPE_COUNT,
            -- Total values (all card types)
            SUM(USD_AMT) AS TOTAL_GTV,
            SUM(NET_REVENUE) AS TOTAL_NET_REVENUE,
            SUM(TOTAL_REVENUE) AS TOTAL_REVENUE,
            ROUND(SUM(TOTAL_REVENUE) / NULLIF(SUM(USD_AMT), 0), 4) AS CARDUP_FEE,
            ROUND(SUM(NET_REVENUE) / NULLIF(SUM(USD_AMT), 0), 4) AS TAKE_RATE,
            -- Visa-specific
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                    ELSE 0
                END
            ) AS GTV_VISA,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN NET_REVENUE
                    ELSE 0
                END
            ) AS NET_REV_VISA,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN TOTAL_REVENUE
                    ELSE 0
                END
            ) AS REV_VISA,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN TOTAL_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS CARDUP_FEE_VISA,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN NET_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Visa' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS TAKE_RATE_VISA,
            -- Mastercard-specific
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                    ELSE 0
                END
            ) AS GTV_MC,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN NET_REVENUE
                    ELSE 0
                END
            ) AS NET_REV_MC,
            SUM(
                CASE
                    WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN TOTAL_REVENUE
                    ELSE 0
                END
            ) AS REV_MC,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN TOTAL_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS CARDUP_FEE_MC,
            ROUND(
                SUM(
                    CASE
                        WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN NET_REVENUE
                        ELSE 0
                    END
                ) / NULLIF(
                    SUM(
                        CASE
                            WHEN CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' THEN USD_AMT
                            ELSE 0
                        END
                    ),
                    0
                ),
                4
            ) AS TAKE_RATE_MC
        FROM
            MAIN
        GROUP BY
            1,
            2
    )
SELECT
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
    IS_COLLECT,
    DATEDIFF(MONTH, FIRST_TX_MONTH, CURRENT_DATE) AS TENURE,
    DATEDIFF(MONTH, LAST_TX_MONTH, CURRENT_DATE) AS RECENCY,
    PAYMENT_TYPE_COUNT,
    -- Overall
    TOTAL_GTV,
    TOTAL_REVENUE,
    TOTAL_NET_REVENUE,
    CARDUP_FEE,
    TAKE_RATE,
    -- Visa
    GTV_VISA,
    REV_VISA,
    NET_REV_VISA,
    CARDUP_FEE_VISA,
    TAKE_RATE_VISA,
    -- Mastercard
    GTV_MC,
    REV_MC,
    NET_REV_MC,
    CARDUP_FEE_MC,
    TAKE_RATE_MC
FROM
    AGG;

select distinct
    CARDUP_PAYMENT_PROMO_CODE_TAG
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T;

select distinct
    ENTITY_NAME
from
    CBM.CARDUP_DB_REPORTING.COMPANY_DATA T1
    left join DEV.SBOX_ADITHYA.SG_GOV_ACRA T2 on T1.UEN = T2.UEN
where
    COMPANY_ID = 3826;

select
    *
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
where
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = 1789;

SELECT
    CARDUP_PAYMENT_PAYMENT_TYPE,
    DATE_TRUNC('QUARTER', CARDUP_PAYMENT_CREATED_AT_UTC_TS) AS QUARTER,
    SUM(CARDUP_PAYMENT_USD_AMT) AS TOTAL_GTV
FROM
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    --AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    --AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
GROUP BY
    CARDUP_PAYMENT_PAYMENT_TYPE,
    DATE_TRUNC('QUARTER', CARDUP_PAYMENT_CREATED_AT_UTC_TS)
ORDER BY
    QUARTER,
    CARDUP_PAYMENT_PAYMENT_TYPE;

select
    *
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
    where CARDUP_PAYMENT_SCHEDULE_ID='415821';

