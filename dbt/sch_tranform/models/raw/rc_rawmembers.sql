{{ config(materialized='incremental', unique_key='MemberID') }}

-- This model transforms the raw/seeded members data into a silver layer

with raw_members as (
    select * from {{ ref('rc_rawmembers_seed') }}
)

select
    MemberCount,
    MemberNo,
    MemberID,
    Surname,
    GivenNames,
    UserName,
    ActiveStatus,
    Registered,
    Verified,
    Referred,
    Email,
    CardType,
    FavouriteStoreName,
    FavouriteStoreID,
    IssuingStoreName,
    IssuingStoreID,
    DateOfBirth,
    RegistrationDateTime,
    RegistrationDate,
    RegistrationTime,
    VerificationDateTime,
    VerificationDate,
    ExpiryDateTime,
    ExpiryDate,
    LastUpdateDate,
    LastUpdateDateTime,
    CreationDate,
    Sex,
    SendEmail,
    SendSMS,
    Phone,
    Mobile,
    State,
    PostCode,
    Address1,
    Address2,
    Suburb,
    Country,
    DeviceID,
    DeviceType,
    PackageName,
    GroupName,
    GroupID,
    PointsAwarded,
    PointsRedeemed,
    MoneyAwarded,
    MoneyRedeemed,
    PointsBalance,
    MoneyBalance,
    LastTxnDate,
    FirstTxnDate,
    LastTxnDateTime,
    FirstTxnDateTime,
    LastAdminTxnDate,
    FirstAdminTxnDate,
    LastAdminTxnDateTime,
    FirstAdminTxnDateTime,
    VerificationType,
    source_name,
    source_row_count
from raw_members


{% if is_incremental() %}
    and MemberID not in (select MemberID from {{ this }})
{% endif %}
