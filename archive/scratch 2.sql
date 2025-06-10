with
    MAIN as (
        select
            *,
            DENSE_RANK() OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID
                order by
                    DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) DESC
            ) as LATEST_MONTH_RANK,
            MIN(DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS))) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_PROMO_CODE
            ) as FIRST_MONTH_PROMO_CODE,
            COUNT(distinct DWH_CARDUP_PAYMENT_ID) OVER (
                partition by
                    CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
                    CARDUP_PAYMENT_PROMO_CODE
            ) as TOTAL_TX_COUNT_PROMO_CODE
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business')
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) <= DATE('2025-02-01')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and CARDUP_PAYMENT_CUSTOMER_COMPANY_ID in (
                '4178',
                '3887',
                '2910',
                '2856',
                '2778',
                '3521',
                '2819',
                '3664',
                '1139',
                '3842',
                '3827',
                '2106',
                '4081',
                '3580',
                '4097',
                '3987',
                '3072',
                '2862',
                '830',
                '4094',
                '3583',
                '2427',
                '4168',
                '3741',
                '3727',
                '3886',
                '4213',
                '2841',
                '2951',
                '3492',
                '3660',
                '3629',
                '3008',
                '3422',
                '3289',
                '3183',
                '4038',
                '4269',
                '3639',
                '3653',
                '4602',
                '3992',
                '3846',
                '3870',
                '4167',
                '1789',
                '1442',
                '3838',
                '2902',
                '3650',
                '3767',
                '2797',
                '1740',
                '414',
                '2556',
                '3857',
                '4233',
                '3601',
                '4126',
                '2858',
                '85',
                '2896',
                '2822',
                '4084',
                '2410',
                '3285',
                '3182',
                '3678',
                '3483',
                '2906',
                '2779',
                '4321',
                '3826',
                '793',
                '3925',
                '3775',
                '4260',
                '616',
                '4057',
                '978',
                '3460',
                '2894',
                '3791',
                '3171',
                '3851',
                '4032',
                '2667',
                '4062',
                '2075',
                '3640',
                '4047',
                '2893',
                '3902',
                '3966',
                '3087',
                '1003',
                '489',
                '3651',
                '2267',
                '451',
                '2127',
                '4302',
                '3086',
                '3446',
                '3688',
                '1746',
                '2335',
                '3814',
                '3174',
                '3048',
                '3395',
                '3524',
                '3286',
                '630',
                '843',
                '4362',
                '3310',
                '4338',
                '4226',
                '3908',
                '3267',
                '3965',
                '4054',
                '1370',
                '2507',
                '3564',
                '4360',
                '4174',
                '3743',
                '3554',
                '4078',
                '3040',
                '3059',
                '2845',
                '3709',
                '3033',
                '3165',
                '3546',
                '3934',
                '4254',
                '3958',
                '2477',
                '3545',
                '4294',
                '4662',
                '4208',
                '1268',
                '3760',
                '2296',
                '344',
                '3695',
                '1784',
                '3244',
                '4383',
                '3024',
                '3573',
                '3667',
                '2249',
                '2244',
                '3260',
                '3348',
                '3711',
                '1628',
                '1066',
                '4086',
                '2308',
                '3001',
                '3649',
                '3357',
                '3237',
                '4209',
                '62',
                '3556',
                '4215',
                '3882',
                '1460',
                '3555',
                '104',
                '4303',
                '3816',
                '2342',
                '4090',
                '3345',
                '4357',
                '3283',
                '3768',
                '3178',
                '3534',
                '3258',
                '2618',
                '3687',
                '2260',
                '4304',
                '2252',
                '3505',
                '2737'
            )
    )
select
    *
from
    MAIN
where
    LATEST_MONTH_RANK = 1;

select
    *
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) <= DATE('2025-02-01')
    and CARDUP_PAYMENT_CU_LOCALE_ID = 1
    and CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = '2822';

select
    *
from
    CBM.CARDUP_DB_REPORTING.COMPANY_DATA
where
    COMPANY_ID = '616';

select
    SUM(CARDUP_PAYMENT_NET_REVENUE_USD_AMT),
    COUNT(distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID)
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) = DATE('2025-02-01')
    and CARDUP_PAYMENT_CU_LOCALE_ID = 1;

select
    CARDUP_PAYMENT_PAYMENT_TYPE,
    SUM(CARDUP_PAYMENT_USD_AMT) as TOTAL_GTV,
    SUM(CARDUP_PAYMENT_CARDUP_FEE / (CARDUP_PAYMENT_LCL_AMT / CARDUP_PAYMENT_USD_AMT)) as TOTAL_CARDUP_FEE,
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) = DATE('2025-02-01')
    and CARDUP_PAYMENT_CU_LOCALE_ID = 1
    and CARDUP_PAYMENT_PROMO_CODE is null
    and CARDUP_PAYMENT_CARD_TYPE = 'Visa'
group by
    1;

with
    MAIN as (
        select
            case
                when CARDUP_PAYMENT_USD_AMT < 50000 then '01. 0-50k'
                when CARDUP_PAYMENT_USD_AMT < 100000 then '02. 50-100k'
                when CARDUP_PAYMENT_USD_AMT < 150000 then '03. 100-150k'
                when CARDUP_PAYMENT_USD_AMT < 200000 then '04. 150-200k'
                when CARDUP_PAYMENT_USD_AMT < 250000 then '05. 200-250k'
                when CARDUP_PAYMENT_USD_AMT < 300000 then '06. 250-300k'
                else '07. 300k+'
            end as USD_AMT_BAND,
            case
                when CARDUP_PAYMENT_NEXT_DAY_FEE > 0 then 'Next Day'
                else 'Standard'
            end as NEXT_DAY,
            CARDUP_PAYMENT_CARD_TYPE,
            case
                when CARDUP_PAYMENT_PROMO_CODE is not null then 'Promo Code'
                else 'No Promo Code'
            end as PROMO_CODE,
            CARDUP_PAYMENT_CUSTOMER_COMPANY_ID,
            DWH_CARDUP_PAYMENT_ID,
            CARDUP_PAYMENT_NET_REVENUE_USD_AMT / CARDUP_PAYMENT_USD_AMT as TAKE_RATE
        from
            ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
        WHERE
            CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
            AND CARDUP_PAYMENT_USER_TYPE IN ('business')
            and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
            and DATE(DATE_TRUNC('month', CARDUP_PAYMENT_CREATED_AT_LCL_TS)) >= DATE('2024-04-01')
            and CARDUP_PAYMENT_CU_LOCALE_ID = 1
            and CARDUP_PAYMENT_CARD_TYPE in ('Visa', 'Mastercard')
    )
select
    USD_AMT_BAND,
    NEXT_DAY,
    CARDUP_PAYMENT_CARD_TYPE,
    PROMO_CODE,
    AVG(TAKE_RATE) as AVG_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.25) as Q1_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.5) as Q2_TAKE_RATE,
    APPROX_PERCENTILE(TAKE_RATE, 0.75) as Q3_TAKE_RATE,
    COUNT(distinct CARDUP_PAYMENT_CUSTOMER_COMPANY_ID) as TOTAL_COMPANIES,
    COUNT(distinct DWH_CARDUP_PAYMENT_ID) as TOTAL_TX_COUNT
from
    MAIN
group by
    1,
    2,
    3,
    4;

select distinct
    CARDUP_PAYMENT_STRIPE_CONNECT,
    CARDUP_PAYMENT_NET_REVENUE_USD_AMT / CARDUP_PAYMENT_USD_AMT as TAKE_RATE,
    CARDUP_PAYMENT_TOTAL_REVENUE_USD_AMT / CARDUP_PAYMENT_USD_AMT as CU_FEE,
    CARDUP_PAYMENT_TOTAL_COST_USD_AMT / CARDUP_PAYMENT_USD_AMT as PROC_COST,
    *
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T
WHERE
    CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding')
    AND CARDUP_PAYMENT_USER_TYPE IN ('business')
    and LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and CARDUP_PAYMENT_CU_LOCALE_ID = 1
    and CARDUP_PAYMENT_CARD_TYPE in ('Visa', 'Mastercard')
    and CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = 590;



select * from dev.sbox_adithya.sg_gov_acra where lower(entity_name) like '%xcel ap%';

select * from cbm.cardup_db_reporting.company_data where uen= '201218021R';