select
    distinct t1.*
from
    ADM.TRANSACTION.CARDUP_PAYMENT_DENORM_T t1
    join CDM.COUNTERPARTY.CARDUP_COMPANY_T t2 on t1.CARDUP_PAYMENT_CUSTOMER_COMPANY_ID = t2.CU_COMPANY_ID
WHERE
    true 
    and CARDUP_PAYMENT_STATUS NOT IN ('Payment Failed', 'Cancelled', 'Refunded', 'Refunding', 'Scheduled')
    --AND CARDUP_PAYMENT_USER_TYPE = 'business'
    AND CARDUP_PAYMENT_CU_LOCALE_ID = 1
    AND LOWER(CARDUP_PAYMENT_PRODUCT_NAME) LIKE '%make%'
    and DATE(DATE_TRUNC('month', date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS))) >= DATE('2025-01-01')
    and DATE(DATE_TRUNC('month', date(CARDUP_PAYMENT_SUCCESS_AT_UTC_TS))) <= DATE('2025-03-01')
    and cardup_payment_customer_company_id in (
        '3286', '3285'
    );

select * from CDM.COUNTERPARTY.CARDUP_COMPANY_T where cu_company_id='193608' limit 10;

--193608
--193607
--193599

select distinct company_id from cbm.cardup_db_reporting.user_data where user_id in ('193608', '193607', '193599');

select distinct cu_company_id from dev.sbox_adithya.sg_gov_acra t1 join CDM.COUNTERPARTY.CARDUP_COMPANY_T t2 on t1.uen = t2.cu_company_uen where t1.entity_name like '%VI STYLE%' and UEN!='53161819J';