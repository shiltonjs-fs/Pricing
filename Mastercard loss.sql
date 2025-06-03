-- This query calculates the total take rate for customers who used promocode for their  Mastercard rent transactions
with
    FILTER_CUSTOMER_MC_RENT as (
        SELECT distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
            and CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard'
    )
select
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
    case
        when CARDUP_PAYMENT_SALES_OWNER LIKE '%jon%'
        or CARDUP_PAYMENT_SALES_OWNER LIKE '%xavier%'
        or CARDUP_PAYMENT_SALES_OWNER LIKE '%terence%'
        or CARDUP_PAYMENT_SALES_OWNER LIKE '%leia%' then 'Managed'
        else 'Unmanaged'
    end as SALES_OWNER,
    SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) / SUM(CARDUP_PAYMENT_USD_AMT) as TAKE_RATE,
    SUM(
        case
            when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_NET_REVENUE_USD_AMT
            else null
        end
    ) / SUM(
        case
            when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_USD_AMT
            else null
        end
    ) as TAKE_RATE_MC_RENT,
    SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) as NET_REVENUE,
    SUM(
        case
            when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_NET_REVENUE_USD_AMT
            else null
        end
    ) as NET_REVENUE_MC_RENT,
    AVG(CARDUP_PAYMENT_USD_AMT) as AVG_GTV,
    COUNT(DWH_CARDUP_PAYMENT_ID) as NUM_TRANSACTIONS,
    COUNT(
        case
            when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then DWH_CARDUP_PAYMENT_ID
            else null
        end
    ) as NUM_TRANSACTIONS_MC_RENT,
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
    join FILTER_CUSTOMER_MC_RENT using (CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
group by
    1,
    2;

-- This query filters the customers whose MC tx take rate is negative
with
    FILTER_CUSTOMER_MC_RENT as (
        SELECT distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
            and CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard'
    ),
    CUSTOMER_LIST as (
        select distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
            join FILTER_CUSTOMER_MC_RENT using (CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
        group by
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        having
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) / SUM(CARDUP_PAYMENT_USD_AMT) < 0
    )
select distinct
    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
    CARDUP_PAYMENT_PAYMENT_TYPE,
    CARDUP_PAYMENT_CARD_TYPE,
    SUM(CARDUP_PAYMENT_USD_AMT) OVER (
        partition by
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            CARDUP_PAYMENT_PAYMENT_TYPE,
            CARDUP_PAYMENT_CARD_TYPE
    ) as TOTAL_GTV_CARDTYPE_PAYTYPE,
    SUM(CARDUP_PAYMENT_USD_AMT) OVER (
        partition by
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    ) as TOTAL_GTV,
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
    join CUSTOMER_LIST using (CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01');

--customer names 
with
    FILTER_CUSTOMER_MC_RENT as (
        SELECT distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
            and CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard'
    ),
    CUSTOMER_LIST as (
        select distinct
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
            join FILTER_CUSTOMER_MC_RENT using (CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
        group by
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        having
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) / SUM(CARDUP_PAYMENT_USD_AMT) < 0
    )
select distinct
    T1.COMPANY_ID,
    T2.ENTITY_NAME
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
    join CUSTOMER_LIST using (CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
    join CBM.CARDUP_DB_REPORTING.COMPANY_DATA T1 on CUSTOMER_LIST.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T1.COMPANY_ID
    join DEV.SBOX_ADITHYA.SG_GOV_ACRA T2 on T1.UEN = T2.UEN
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01');

--This query gets the customer's first transaction whether it's promo or not
WITH
    FILTER_CUSTOMER_MC_RENT AS (
        SELECT DISTINCT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE = 'business'
            AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
            AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            AND DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-01-01')
            AND CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
            AND CARDUP_PAYMENT_CARD_TYPE = 'Mastercard'
    ),
    FILTERED_TRANSACTIONS AS (
        SELECT
            T.*,
            ROW_NUMBER() OVER (
                PARTITION BY
                    T.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                ORDER BY
                    T.CARDUP_PAYMENT_CREATED_AT_LCL_TS
            ) AS TXN_RANK
        FROM
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T
            JOIN FILTER_CUSTOMER_MC_RENT F ON T.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = F.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
    ),
    FIRST_TRANSACTIONS as (
        SELECT
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            CARDUP_PAYMENT_PROMO_CODE_TYPE,
            DATEDIFF('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS, CURRENT_DATE()) AS TENURE
        FROM
            FILTERED_TRANSACTIONS
        WHERE
            TXN_RANK = 1
    ),
    MAIN as (
        select
            T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            T3.CARDUP_PAYMENT_PROMO_CODE_TYPE,
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) / SUM(CARDUP_PAYMENT_USD_AMT) as TAKE_RATE,
            SUM(
                case
                    when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
                    and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_NET_REVENUE_USD_AMT
                    else null
                end
            ) / SUM(
                case
                    when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
                    and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_USD_AMT
                    else null
                end
            ) as TAKE_RATE_MC_RENT,
            SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT) as NET_REVENUE,
            SUM(
                case
                    when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
                    and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then CARDUP_PAYMENT_NET_REVENUE_USD_AMT
                    else null
                end
            ) as NET_REVENUE_MC_RENT,
            AVG(CARDUP_PAYMENT_USD_AMT) as AVG_GTV,
            COUNT(DWH_CARDUP_PAYMENT_ID) as NUM_TRANSACTIONS,
            COUNT(
                case
                    when CARDUP_PAYMENT_PROMO_CODE LIKE '%SAVERENT%'
                    and CARDUP_PAYMENT_CARD_TYPE = 'Mastercard' then DWH_CARDUP_PAYMENT_ID
                    else null
                end
            ) as NUM_TRANSACTIONS_MC_RENT
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T T1
            join FILTER_CUSTOMER_MC_RENT T2 on T2.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID=T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
            join FIRST_TRANSACTIONS T3 on T1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = T3.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
        group by
            1,2
    )
select
    *
from
    FIRST_TRANSACTIONS;