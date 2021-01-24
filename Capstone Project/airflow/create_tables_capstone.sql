
DROP TABLE IF EXISTS public.i94;
DROP TABLE IF EXISTS public.temperatures;
DROP TABLE IF EXISTS public.airports_data;
DROP TABLE IF EXISTS public.demographics;
DROP TABLE IF EXISTS public.states_code;
DROP TABLE IF EXISTS public.countries_code;
DROP TABLE IF EXISTS public.modes_code;
DROP TABLE IF EXISTS public.visas_code;
DROP TABLE IF EXISTS public.airports_code;
DROP TABLE IF EXISTS public.time;
DROP TABLE IF EXISTS public.visitors;
DROP TABLE IF EXISTS public.visit;
DROP TABLE IF EXISTS public.visa;
DROP TABLE IF EXISTS public.flag;
DROP TABLE IF EXISTS public.fact;


CREATE TABLE public.i94 (
	cicid int4 NOT NULL,
	date_created date,
    year int,
    month int,
    day int,
    citizenship int,
    resident int,
    age int,
    birth_year int,
    gender char(1),
    occupation char(10),
    allowed_stay_till date,
    allowed_stay_days int4,
    stayed_days int4, 
    visa_class int4,
    visa_type char(5),
    port varchar(256),
    mode int4,
    arraval_state char(5),
    visa_issued_by varchar(256),
    arrival_flag char(5),
    depart_flag char(5),
    update_flag char(5),
    match_flag char(5),
    airline varchar(256),
    admission_number int4,
    flight_number varchar(256)
);

CREATE TABLE public.temperatures (
	id int NOT NULL,
    avg_temp float,
    sd_temp float,
    city varchar(256),
    country varchar(256),
    month int,
    day int,
    CONSTRAINT id_pkey PRIMARY KEY (id)
);

CREATE TABLE public.airports_data (
	ident char(15) NOT NULL,
	type varchar(256),
	name varchar(256),
	elevation_ft int4,
	iso_country char(4),
    municipality varchar(256),
    gps_code char(5),
    airport_code char(5),
    coordinates varchar(256),
    state char(2),
	CONSTRAINT ident_pkey PRIMARY KEY (ident)
);


CREATE TABLE public.demographics (
	city varchar(256),
	median_age float,
	male_population int8,
	female_population int8,
	population int8,
	num_veterans int8,
	foreign_born int8,
	avg_household_size float,
	state char(2),
	race varchar(256),
	count int8,
    CONSTRAINT city_pkey PRIMARY KEY (city)
);


CREATE TABLE public.states_code (
	code varchar(256),
	name varchar(256),
    CONSTRAINT states_code_pkey PRIMARY KEY (code)
);

CREATE TABLE public.countries_code (
	code int,
	name varchar(256),
    CONSTRAINT countries_code_pkey PRIMARY KEY (code)
);

CREATE TABLE public.modes_code (
	code int,
	name varchar(256),
    CONSTRAINT modes_code_pkey PRIMARY KEY (code)
);

CREATE TABLE public.visas_code (
	code int,
	name varchar(256),
    CONSTRAINT visas_code_pkey PRIMARY KEY (code)
);

CREATE TABLE public.airports_code (
	code varchar(256),
	name varchar(256),
    CONSTRAINT airport_code_pkey PRIMARY KEY (code)
);


CREATE TABLE public."time" (
	date_created date,
	"hour" int4,
	"day" int4,
	week int4,
	"month" varchar(256),
	"year" int4,
	weekday varchar(256),
	CONSTRAINT time_pkey PRIMARY KEY (date_created)
);

CREATE TABLE public.visitors (
	cicid int4 NOT NULL,
	citizenship int,
	resident int,
    age int,
	gender char(1),
	birth_year int,
    occupation varchar(256),
	CONSTRAINT cicid_visitor_pkey PRIMARY KEY (cicid)
);

CREATE TABLE public.visit (
	cicid int4 NOT NULL,
    allowed_stay_till date,
    allowed_stay_days int4,
	stayed_days int4, 
    CONSTRAINT cicid_visit_pkey PRIMARY KEY (cicid)
);

CREATE TABLE public.visa (
	cicid int4 NOT NULL,
    visa_class int4,
    visa_type char(5),
    mode int4,
    visa_issued_by varchar(256),
    CONSTRAINT cicid_visa_pkey PRIMARY KEY (cicid)
    
);


CREATE TABLE public.flag (
	cicid int4 NOT NULL,
	arrival_flag char(5),
    depart_flag char(5),
    update_flag char(5),
    match_flag char(5),
    CONSTRAINT cicid_flag_pkey PRIMARY KEY (cicid)
);


CREATE TABLE public.fact (
	cicid int4 NOT NULL,
    date_created date,
    admission_number int4,
    port varchar(256)
);