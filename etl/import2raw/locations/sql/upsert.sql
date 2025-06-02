-- Upsert from temp_rc_rawlocations to rc_rawlocations in Redshift
-- Assumes temp_rc_rawlocations has the same schema as rc_rawlocations
-- Adjust the column list and key columns as needed

BEGIN;

DELETE FROM public.rc_rawlocations
USING public.temp_rc_rawlocations t
WHERE rc_rawlocations.StoreID = t.StoreID;

INSERT INTO public.rc_rawlocations (
    StoreID, LocationName, LocationOwnerType, ABN, State, StoreType, StoreTypeName, DateOpened, DateClosed, ShortCode, TradeDays, Territory, Country, Postcode, Address1, Address2, Suburb, Phone, Latitude, Longitude, AvgOrderTime, Bitmask, PaymentGatewayCustomerID, OpeningTimesSunday, OpeningTimesMonday, OpeningTimesTuesday, OpeningTimesWednesday, OpeningTimesThursday, OpeningTimesFriday, OpeningTimesSaturday, OpeningTimesPublicHoliday, ClosingTimesSunday, ClosingTimesMonday, ClosingTimesTuesday, ClosingTimesWednesday, ClosingTimesThursday, ClosingTimesFriday, ClosingTimesSaturday, ClosingTimesPublicHoliday
)
SELECT
    StoreID, LocationName, LocationOwnerType, ABN, State, StoreType, StoreTypeName,
    NULL as DateOpened, 
    NULL as DateClosed,
    ShortCode, TradeDays, Territory, Country, Postcode, Address1, Address2, Suburb, Phone, Latitude, Longitude, AvgOrderTime, Bitmask, PaymentGatewayCustomerID, OpeningTimesSunday, OpeningTimesMonday, OpeningTimesTuesday, OpeningTimesWednesday, OpeningTimesThursday, OpeningTimesFriday, OpeningTimesSaturday, OpeningTimesPublicHoliday, ClosingTimesSunday, ClosingTimesMonday, ClosingTimesTuesday, ClosingTimesWednesday, ClosingTimesThursday, ClosingTimesFriday, ClosingTimesSaturday, ClosingTimesPublicHoliday
FROM public.temp_rc_rawlocations;

DROP TABLE public.temp_rc_rawlocations;

COMMIT;
