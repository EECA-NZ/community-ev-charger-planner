set role eeca_poweruser;

--drop all tables

drop table if exists pop_vehicles_sub1;
drop table if exists pop_vehicles_sub2;
drop table if exists pop_vehicles;
drop table if exists curr_ev_1000;
drop table if exists sa2connector;
drop table if exists popnbase;
drop table if exists popngrowth;
drop table if exists statsnz_sa2_1;

--create pop_vehicles_sub1 table with the correct field names and data types

CREATE TABLE eeca_ev.pop_vehicles_sub1 (
	sa2_id int4 null,
	sa2name varchar(50) NULL,
	"year" int4 NULL,
	populationestimate int4 NULL,
	populationgrowth int4 null,
	current_ev int4 NULL,
	current_phev int4 NULL,
	current_other int4 NULL,
	current_vehicles int4 NULL,
	proj_ev int4 NULL,
	ev_1000_curr numeric(8, 3) null,
	ev_1000 numeric(8, 3) null,
	ev_percent numeric(8,3) null,
	vehicles_1000 numeric(8, 3) null,
	popn_ev numeric(8, 3) null
);

--insert the sa2 name and population estimate by year into the pop_vehicles_sub1 table from the population projections table

insert into pop_vehicles_sub1 select
0 as sa2_id,
sa2name2022 as sa2name,
"year",
populationestimate,
0 as populationgrowth,
0 as current_ev,
 0 as proj_ev,
 0 as ev_1000_curr,
 0 as ev_1000,
 0 as popn_ev
FROM sa2popnprojections;

--exclude sa2s not included in analysis ie those with a high number of EVs as company or fleet vehicles--
--this list was created in excel based mainly on SA2s with high EV/1000 but also SA2s with low population. Both indicating high commercial/industrial--
--ideally the extract from NZTA would be private vehicles only

delete from pop_vehicles_sub1
using sa2_exclude_list
where pop_vehicles_sub1.sa2name = sa2_exclude_list.sa2_name;

/*add sa2 numbers*/

update pop_vehicles_sub1
	set sa2_id = sa2_v1_00::int4
	from statsnz_sa2 
where pop_vehicles_sub1.sa2name = statsnz_sa2.sa2_v1_00_name_ascii;

--add NZTA vehicles count data from num_ev table which was created from the NZTA data extract--
--The NZTA extract is a file sent quarterly to us in excel format. It needs some editing before being loaded as a table--

UPDATE pop_vehicles_sub1 s
SET 
  current_ev = NULLIF(n.bev,0),
  current_phev = nullif(n.phev,0),
  current_other = nullif(n.other,0),
  current_vehicles = nullif(n.bev,0) + nullif(n.phev,0) + nullif(n.other,0)
  FROM num_ev n
WHERE n.sa2name = s.sa2name ;

--Create a table with the current EV/1000 
create table curr_ev_1000 as
select * from pop_vehicles_sub1
where "year" = 2023;

--calculate ev_1000 current for the most recent year of actual data--
--this is used later to scale up for population growth--

UPDATE pop_vehicles_sub1 s
SET ev_1000_curr = CAST(s.current_ev AS numeric(8, 3)) / (
    SELECT NULLIF(CAST(s.populationestimate AS numeric) / 1000, 0)
    FROM curr_ev_1000 c
    WHERE s.sa2name = c.sa2name);

--calculate popn_ev field. This is a step in the calculation of future ev per SA2 by multiplying
--the current EV/1000 by the future population estimate per SA2

UPDATE pop_vehicles_sub1
SET popn_ev = 
cast(ev_1000_curr as numeric (8,3)) *
(CAST(populationestimate as numeric)/ 1000)
where "year" > 2023;

-- set 2023 to the current EV/1000 

update pop_vehicles_sub1 
set ev_1000 = ev_1000_curr 
where "year" = 2023;

-- set 2023 to the current vehicles/1000 

update pop_vehicles_sub1 
set vehicles_1000 = 
current_vehicles::numeric/ nullif(populationestimate::numeric /1000,0)
where "year" = 2023;

-- set 2023 to the current EV percent 

update pop_vehicles_sub1 
set ev_percent = 
current_ev::numeric / nullif(current_vehicles:: numeric ,0)*100
where "year" = 2023;

--create table pop_vehicles_sub2 by joining the pop_vehicles_sub1 table
--to climate change commission EV projection numbers table.
--This table was created from a spreadsheet that the CCC published in 2021.
--This brings in their projected total nationwide number of EVs per year
--which we can then allocate to SA2s based on current EV penetration
--If there are updated estimates of EVs over the coming years then they can be brought in at this point

create table pop_vehicles_sub2 as
SELECT s."year",
SUM(s.popn_ev) as sum_popn_ev,
max(p.proj_num_ev) as num_ev,
0.123 as popn_ev_ratio
FROM pop_vehicles_sub1 s
join ccc_proj_num_ev p on s."year" = p."year"
GROUP BY s."year";

--this creates a scaling factor popn_ev_ratio

update pop_vehicles_sub2
set popn_ev_ratio = num_ev/nullif(cast(sum_popn_ev as numeric),0);

--Apply the scaling factor to popn_ev to calculate a projected number of EVs proj_ev
UPDATE pop_vehicles_sub1 s
SET proj_ev = 
	popn_ev::numeric *
	z.popn_ev_ratio::numeric
	from pop_vehicles_sub2 z 
	where s."year" = z."year";

--Set the current number of EVs and EV/1000 for 2023
update pop_vehicles_sub1
set proj_ev = current_ev 
where "year" = 2023;

UPDATE pop_vehicles_sub1
SET ev_1000 = 
	proj_ev /(populationestimate::numeric / 1000)
where "year" >2023;

--create summary of connectors per SA2

create table sa2connector as
select sa2, sa2_name, sa2_name_ascii, count(*) as numconnector
from evroam_charge_connector
group by sa2, sa2_name, sa2_name_ascii;

--add connectors to table

CREATE TABLE pop_vehicles AS
SELECT	
	row_number () over () as pop_vehicles_id
	, sa2_id             as sa2_id
	, sa2name            as sa2name
	, year               as year
	, populationestimate as populationestimate
	, populationgrowth   as populationgrowth
	, coalesce(current_ev ,0)        as current_ev
	, coalesce(current_phev ,0)      as current_phev
	, coalesce(current_other ,0)     as current_other
	, coalesce(current_vehicles ,0)  as current_vehicles
	, coalesce(proj_ev ,0)           as proj_ev
	, coalesce(ev_1000_curr ,0)      as ev_1000_curr
	, coalesce(ev_1000 ,0)           as ev_1000
	, coalesce(ev_percent ,0)        as ev_percent
	, coalesce(vehicles_1000 ,0)     as vehicles_1000
	, coalesce(popn_ev ,0)           as popn_ev
	, coalesce(proj_ev::numeric ,0)/60 as connectorsneeded
	, coalesce(s.numconnector, 0) AS numconnector
	, coalesce((coalesce(proj_ev::numeric ,0)/60) - numconnector ,0) as connectorsgap
FROM pop_vehicles_sub1 t
LEFT JOIN sa2connector s ON t.sa2name = s.sa2_name_ascii
where coalesce(t.sa2_id,0) > 0
order by t.sa2_id;

--Calculate the connectors gap as the difference between connectors needed for each year and the current number of connectors
update pop_vehicles 
	set connectorsgap = connectorsneeded - numconnector;

--Add primary key to table

ALTER TABLE pop_vehicles
ADD CONSTRAINT pk_pop_vehicles PRIMARY KEY (pop_vehicles_id);

--add demographic information

alter table pop_vehicles 
add column ppl_occupation int4,
add column managers int4,
add column professionals int4,
add column tech_trade int4,
add column hhincome numeric;

update pop_vehicles 
set 
	ppl_occupation = s.ppl_occupation,
	managers = s.managers,
	professionals = s.professionals,
	tech_trade = s.tech_trade,
	hhincome = s.hhincome
from sa2_demographics s
where pop_vehicles.sa2name = s.sa2;

--calculate increase in population

create table popnbase as
select sa2name, populationestimate as popnbase
from pop_vehicles
where "year" = 2023;

create table popngrowth as
select s.sa2name,
	s."year", 
    s.populationestimate,
    s.populationestimate - p.popnbase as populationgrowth
from pop_vehicles s
join popnbase p on s.sa2name = p.sa2name
order by sa2name, "year";

update pop_vehicles 
set 
	populationgrowth = s.populationgrowth
from popngrowth s
where pop_vehicles."year" = s."year"
	and pop_vehicles.sa2name = s.sa2name;

--Create an SA2 table

create table statsnz_sa2_1 as
	select 
	statsnz_sa2_id,
	geom,
	sa2_v1_00::int4 as sa2_id,
	sa2_v1_00_name as sa2_name,
	sa2_v1_00_name_ascii as sa2_name_ascii,
	land_area_sq_km,
	area_sq_km,
	shape_length,
	shape_area
	from eeca_ev.statsnz_sa2
order by sa2_id;

drop table if exists popnbase;
drop table if exists popngrowth;

commit;

reset role;


