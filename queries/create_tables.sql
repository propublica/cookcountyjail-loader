create table if not exists inmates (
    Id serial primary key,
    Age_At_Booking integer,
    Bail_Amount varchar(512) null,
    Booking_Date date,
    Booking_Id varchar(12),
    Charges text null,
    Court_Date varchar(32),
    Court_Location text null,
    Gender varchar(32),
    Height varchar(16),
    Housing_Location varchar(512),
    Incomplete boolean,
    Inmate_Hash varchar(512),
    Race varchar(16),
    Scrape_Date date,
    Weight varchar(16)
);
