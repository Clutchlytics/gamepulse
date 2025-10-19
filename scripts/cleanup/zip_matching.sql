-- ================================================
--  Zip Code Realism Mapping Note
-- ================================================
-- This reference table was created to enhance geographic realism in synthetic datasets.
-- Since Faker-generated city, state, and zip combinations may not align with real-world data,
-- ZipMapping provides verified pairings to ensure consistency and plausibility.
-- This mapping supports downstream validation, enrichment, and location-based analytics
-- by anchoring fake records to realistic U.S. postal regions.
-- ================================================


 TABLE ZipMapping (
    City VARCHAR(100),
    State VARCHAR(2),
    ValidZip CHAR(5)
);

INSERT INTO ZipMapping (City, State, ValidZip)
VALUES 
('Chicago', 'IL', '60601'),
('Oklahoma City', 'OK', '73102'),
('Lubbock', 'TX', '79401'),
('Dallas', 'TX', '75201'),
('Baton Rouge', 'LA', '70801'),
('Miami', 'FL', '33101'),
('Little Rock', 'AR', '72201'),
('Albuquerque', 'NM', '87101'),
('Los Angeles', 'CA', '90001'),
('Fort Worth', 'TX', '76102'),
('Phoenix', 'AZ', '85001'),
('Arlington', 'TX', '76010'),
('New York', 'NY', '10001'),
('Corpus Christi', 'TX', '78401'),
('San Antonio', 'TX', '78205'),
('Houston', 'TX', '77002'),
('Plano', 'TX', '75023'),
('El Paso', 'TX', '79901'),
('Austin', 'TX', '78701'),
('Denver', 'CO', '80202');

SELECT * FROM ZipMapping

