-- This model transforms the raw/seeded members data into a silver layer

with raw_members as (
    select * from {{ ref('members_validated') }}
)

select
    MemberID,
    Surname,
    GivenNames,
    Email,
    DateOfBirth,
    RegistrationDateTime,
    Verified,
    ActiveStatus,
    -- Add more columns and transformations as needed
    source_name,
    source_row_count
from raw_members
where ActiveStatus = 1 and Verified = 1
