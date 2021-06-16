import pyodbc
import csv
import psycopg2
import pandas as pd

ymd = str(input('วันที่วิ่งสำรวจ='))
run = str(input('หมายเลขRun='))

'''drop table access_key,access_pic,access_valuelaser,data_suvey,gps_lost,
survey,survey_point,survey_point_local,survey_image'''

pyodbc.lowercase = False
path_mdb = 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\202102%s_%s_edit.mdb' % (ymd, run, ymd, run)
conPG = pyodbc.connect(
    r"Driver={Microsoft Access Driver (*.mdb, *.accdb)};" +
    r"Dbq=%s;" % (path_mdb))

a = path_mdb[45:55]

####################################################################################################################################
cre_valuelaser = "SELECT a.CHAINAGE as chainage, LONGITUDE as lon, LATITUDE as lat, \
        \nRWP_IRI AS iri_right, LWP_IRI AS iri_left,  (((RWP_IRI)+(LWP_IRI))/2) as iri  ,LANE_IRI AS iri_lane, \
        \nRUT_EDG_SE  AS rutt_right, RUT_CTR_SE AS rutt_left, RUT_SE AS rutting, \
        \nLANE_MPD AS texture, ((LANE_MPD)*0.8)+0.008 as etd_texture, \
        \n'%s' as file_name \
        \n\nFROM (((GPS_Processed_%s AS a) \
        \nLEFT JOIN Profiler_IRI_%s as b on a.CHAINAGE = b.CHAINAGE) \
        \nleft join TPL_Processed_%s as c on a.CHAINAGE = c.CHAINAGE) \
        \nleft join Profiler_MPD_%s as d on a.CHAINAGE = d.CHAINAGE" % (a, a, a, a, a)
cur = conPG.cursor()
sql = cre_valuelaser
cur.execute(sql)

with open('D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_valuelaser.csv' % (ymd, run), 'w', newline='') as f:
    writer = csv.writer(f)
    for row in cur.fetchall():
        writer.writerow(row)
cur.close()
######################################################################################################################################

cre_key = "SELECT CHAINAGE_START as event_str, CHAINAGE_END as event_end, EVENT AS event_num, \
        \nSWITCH_GROUP as event_type, EVENT_DESC as event_name, link_id, section_id, km_start, km_end, length, \
        \nlane_no, survey_date,LATITUDE_START as lat_str, LATITUDE_END as lat_end, LONGITUDE_START as lon_str, LONGITUDE_END as lon_end \
        \nfrom KeyCode_Raw_%s" % (a)

cur = conPG.cursor()
sql = cre_key
cur.execute(sql)

with open('D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_key.csv' % (ymd, run), 'w', newline='') as f:
    writer = csv.writer(f)
    for row in cur.fetchall():
        writer.writerow(row)
cur.close()
######################################################################################################################################

cre_pic = "select CHAINAGE as chainage_pic, FRAME as  frame_number \
        \nfrom Video_Processed_%s_1" % (a)

cur = conPG.cursor()
sql = cre_pic
cur.execute(sql)

with open('D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_pic.csv' % (ymd, run), 'w', newline='') as f:
    writer = csv.writer(f)
    for row in cur.fetchall():
        writer.writerow(row)
cur.close()

print('STEPconventer:access to CSV file successfully')

conPG = psycopg2.connect(
    host="localhost",
    database="postgres",
    user="postgres",
    password="123456",
    port="5432"
)
print("STEPconnect:Database opened successfully")

##STEP_0  'Create Table access'####################################################################################################################################
table_valuelaser = '''
CREATE TABLE access_valuelaser
		(
		  chainage real, lon real, lat real, iri_right text, iri_left text,
		  iri text, iri_lane numeric(8,2), rutt_right numeric(8,2), rutt_left numeric(8,2),
		  rutting numeric(8,2), texture numeric(8,2), etd_texture numeric(8,2), file_name char(20)
		)
	'''
cur_valuelaser = conPG.cursor()
cur_valuelaser.execute(table_valuelaser)

table_key = '''
CREATE TABLE access_key
		(
		  event_str real, event_end real, event_num char(2), event_type char(20), event_name char(20),
		  link_id char(20), section_id char(50), km_start char(50), km_end char(50),  length char(100),
		  lane_no char(50), survey_date char(50),
		  lat_str real, lat_end real, lon_str real, lon_end real
		)
    '''
cur_key = conPG.cursor()
cur_key.execute(table_key)

table_pic = '''
CREATE TABLE access_pic
		(
		  chainage_pic real,
		  frame_number char(20)
		  )
		  '''
cur_pic = conPG.cursor()
cur_pic.execute(table_pic)

print('STEP0:create table access successfully')

# ##STEP_1  'insert into data to table_access'####################################################################################################################################
insert_table = "COPY access_key \
\n\tFROM 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_key.csv' \
\n\tDELIMITER ',' CSV; \
\nCOPY access_pic \
\n\tFROM 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_pic.csv' \
\n\tDELIMITER ',' CSV; \
\nCOPY access_valuelaser \
\n\tFROM 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\access_valuelaser.csv' \
\n\tDELIMITER ',' CSV; " % (ymd, run, ymd, run, ymd, run)

cur_step1 = conPG.cursor()
cur_step1.execute(insert_table)
print("STEP1:insert data successfully")

##STEP_2.1  create table 'data_suvey'####################################################################################################################################
merge_csv = '''
create table data_suvey as 
		select a.*, status, status_type
		from 
				(
					select replace(survey_date, ' ','')::date as date, a.*, b.*,
					st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
					from access_key a, 
					(select *
					from 
						(
							select *, ((iri_right::double precision + iri_left::double precision)/2)::text as iri
							from 
							(
								SELECT chainage, lon, lat,
								case when iri_right::real > 8 then (random_between(7.00, 8.00))::text else iri_right end iri_right, 
								case when iri_left::real > 8 then (random_between(7.00, 8.00))::text else iri_left end iri_left, 
								iri as iri_old, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture, file_name
								FROM access_valuelaser
							) foo
						) a
					left join access_pic b on  chainage = chainage_pic::int) b
					where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
				) a
		inner join
				(
				select *
				from 
				(	
					select *, case when status = 'มีGPSlost' and count < 11 then 'ดำเนินการต่อ'
					when status = 'มีGPSlost' and count > 10 then '***ไม่ดำเนินการต่อ' else 'ดำเนินการต่อ' end status_type
					from 
					(	--เชค ข้อมูล ต้องเท่ากับจำนวน Link_id ใน Key_code
						select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
						'มีGPSlost'::text status
						from (
							select a.*, b.*
							from access_key a, 
							(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
							where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
							and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''			
							) foo
						where (lon = 0 or lat = 0) and link_id is not null
						group by file_name, link_id, replace(survey_date, ' ','')::date
						union 
						select a.*
						from 
						(
							select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
							'ไม่มีGPSlost'::text status
							from (
								select a.*, b.*
								from access_key a, 
								(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
								where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
								and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''			
								) foo
							where (lon != 0 or lat != 0) --and replace(survey_date, ' ','') > '2021-01-18'
							group by file_name, link_id, replace(survey_date, ' ','')::date
						) a
						left join
						(
							select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count,
							'มีGPSlost'::text status
							from (
								select a.*, b.*
								from access_key a, 
								(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
								where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
								and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''			
								) foo
							where (lon = 0 or lat = 0) 
							group by file_name, link_id, replace(survey_date, ' ','')::date
						) b on a.link_id = b.link_id
						where b.link_id is null and a.link_id is not null
						order by link_id
					) foo
				) foo
				where status_type = 'ดำเนินการต่อ'
		) b on a.link_id = b.link_id'''
cur_step21 = conPG.cursor()
cur_step21.execute(merge_csv)
print("STEP2.1:merge CSV_access to table 'data_survey_run' successfully")
conPG.commit()

##STEP_2.2  chcek_gps_loss ####################################################################################################################################
gps_loss = '''create table gps_lost as
		select a.*, b.chainage, min-6 as new_p_min, max+6 as new_p_max
		from
		(
			--max min ช่วง GPS ที่เริ่มหาย
			select min(chainage) as min, max(chainage) as max, count(*) as c_p, count(*)*5 as meter,
			link_id, date, grp2
			from 
			(
				select chainage, lat, lon, event_str, event_end, link_id, date,
				row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp1, 
				row_number() over (partition by date, link_id order by  chainage)  -   --(minus)
				row_number() OVER (partition by lat, lon, link_id order by  chainage) as grp2
				from data_suvey
				order by chainage
			) foo
			where (lon = 0 or lat = 0)
			group by grp2, link_id, date, grp2
			order by min
		) a,
		(
		select chainage, lat, lon, event_str, event_end, link_id, date
		from data_suvey
		where split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' and (lon = 0 or lat = 0)
		and status = 'มีGPSlost'
		) b 
		where chainage between min and max 
		group by min, max, c_p, meter, a.link_id, a.date, grp2, b.chainage, new_p_min, new_p_max
		order by min'''
cur_step22 = conPG.cursor()
cur_step22.execute(gps_loss)
print("STEP2.2:create table GPS_lost successfully ")
print('')
conPG.commit()

# ##STEP_3.1  chcek_gps_loss ####################################################################################################################################
# querry_csv = '''
# 		select *
# 		from
# 		(
# 			select file_name, replace(survey_date, ' ','')::date as date,  link_id, count(*) as count
# 			from (
# 				select a.*, b.*
# 				from access_key a,
# 				(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
# 				where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
# 				and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''
# 				) foo
# 			where (lon = 0 or lat = 0) --and replace(survey_date, ' ','') > '2021-01-18'
# 			group by file_name, link_id, replace(survey_date, ' ','')::date
# 		) foo
# 		where count < 11 '''
# cur_step31 = conPG.cursor()
# cur_step31.execute(querry_csv)
# conPG.commit()
# my_table = pd.read_sql(querry_csv, conPG)
# print('###############link_id ที่มี GPS lost มากกว่า 10 ต้องได้รับการแก้ไข:##################' )
# print(my_table)
# print('______________________________________________________________________')
# print('')
#
# ##STEP_3.2  chcek_gps_loss ####################################################################################################################################
#
# str_end = '''
# select *
# 		from
# 		(
# 			select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom
# 			from
# 			( --sum event
# 				select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
# 				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
# 				from data_suvey a
# 				--where link_id = '33600040201L2CC01   '
# 				group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
# 				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
# 				union
# 				select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
# 				st_setsrid(st_makepoint(lon_end:\\infraplus:real ,lat_end:\\infraplus:real),4326) as the_geom, file_name, 'end' as direction
# 				from data_suvey a
# 				--where link_id = '33600040201L2CC01   '
# 				group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
# 				st_setsrid(st_makepoint(lon_end:\\infraplus:real ,lat_end:\\infraplus:real),4326), file_name
# 			) even
# 			left join
# 			(
# 				select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
# 				from access_key a,
# 				(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
# 				where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
# 				and replace(link_id, ' ', '') != ''
# 			) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
# 			where even.chainage - b.chainage > 6
# 			group by even.chainage, direction, event_name, link_id, even.the_geom
# 			order by even.chainage, link_id
# 		) foo
# 		where chainage_data is null'''
# cur_step32 = conPG.cursor()
# cur_step32.execute(str_end)
# conPG.commit()
# my_table1 = pd.read_sql(str_end, conPG)
# print('###############link_id ที่มีปัญหา km_str และ km_end :##################' )
# print(my_table1)
# print('______________________________________________________________________')
# print('')
#
# ##STEP_3.3  chcek_gps_loss รอยต่อ  ####################################################################################################################################
# gps_lo = '''
# select *
# 		from
# 		(
# 			select a.chainage as gps_lost_ch, c_p, meter, a.link_id, a.date,
# 			min(b.chainage) as chainage_str, max(b.chainage) as chainage_end,
# 			(max(b.chainage) -min(b.chainage))::real as persent,
# 			--(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
# 			ST_MakeLine(the_geom ORDER BY b.chainage) as the_geom, file_name
# 			from gps_lost a, data_suvey b
# 			where b.chainage between new_p_min and new_p_max and lon != 0
# 			and st_y(the_geom) > 0  and status = 'มีGPSlost'
# 			group by gps_lost_ch, c_p, meter, a.link_id, a.date, file_name
# 		) foo
# 		where persent = 0'''
# cur_step33 = conPG.cursor()
# cur_step33.execute(gps_lo)
# conPG.commit()
# my_table2 = pd.read_sql(gps_lo, conPG)
# print('###############Check Gps_lost ช่วงรอยต่อระหว่าง link_id :##################' )
# print(my_table2)
# print('______________________________________________________________________')
# print('')
#
# ##STEP_3.4  chcek iri rut mpd and pic  ####################################################################################################################################
# irmp = '''
# select *
# 		from data_suvey
# 		where iri is null or rutting is null or texture is null or frame_number is null
# 		and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = '' '''
# cur_step34 = conPG.cursor()
# cur_step34.execute(irmp)
# conPG.commit()
# my_table3 = pd.read_sql(irmp, conPG)
# print('###############chcek  ค่า : iri rut mpd and pic :##################' )
# print(my_table3)
# print('______________________________________________________________________')
print('')

##STEP_4.1  update position str and end (even) : END  ####################################################################################################################################
step41 = '''
update data_suvey a set lat_end = b.lat , lon_end = b.lon --(lat_str = b.lat , lon_str = b.lon // lat_end = b.lat , lon_end = b.lon
	from ( 
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select min(chainage_even) as ch_str, max(chainage_even) as ch_end, chainage,
			(chainage-min(chainage_even))::real / (max(chainage_even)-min(chainage_even))::real as persent,
			ST_MakeLine(the_geom ORDER BY chainage) as the_geom
			from 
			(
				select a.chainage, b.chainage as chainage_even, the_geom
				from
				(select chainage
				from 
				( --เชค  
					select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom
					from 
					( --sum event
						select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
						union
						select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name, 'end' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
					) even	
					left join 
					(	
						select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
						from access_key a, 
						(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
						where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
						and replace(link_id, ' ', '') != ''
					) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
					--where even.chainage - b.chainage > 6
					group by even.chainage, direction, event_name, link_id, even.the_geom
					order by even.chainage, link_id
					
				) foo
				group by chainage order by chainage) a,
				(select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
				from access_valuelaser where lat != 0 and lon != 0) b 
				where a.chainage between  b.chainage -5 and b.chainage+5
			) foo
			group by chainage
		) foo
	) b
	where b.chainage = event_end
	--chainage = event_str // chainage = event_end
'''
cur_step41 = conPG.cursor()
cur_step41.execute(step41)
conPG.commit()

##STEP_4.2  update position str and end (even) : STR  ####################################################################################################################################
step42 = '''
update data_suvey a set lat_str = b.lat , lon_str = b.lon --(lat_str = b.lat , lon_str = b.lon // lat_end = b.lat , lon_end = b.lon
	from ( 
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(
			select min(chainage_even) as ch_str, max(chainage_even) as ch_end, chainage,
			(chainage-min(chainage_even))::real / (max(chainage_even)-min(chainage_even))::real as persent,
			ST_MakeLine(the_geom ORDER BY chainage) as the_geom
			from 
			(
				select a.chainage, b.chainage as chainage_even, the_geom
				from
				(select chainage
				from 
				( --เชค  
					select even.chainage, min(b.chainage) as chainage_data, direction, event_name, link_id, even.the_geom
					from 
					( --sum event
						select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name, 'str' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
						st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
						union
						select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name, 'end' as direction
						from data_suvey a
						--where link_id = '33600040201L2CC01   '
						group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
						st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
					) even	
					left join 
					(	
						select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
						from access_key a, 
						(select * from access_valuelaser left join access_pic b on  chainage = chainage_pic::int) b
						where chainage between (event_str-5) and (event_end+5) and replace(event_type, ' ', '') = 'pavetype.'
						and replace(link_id, ' ', '') != ''
					) b on st_dwithin(even.the_geom, b.the_geom, 0.00004)
					--where even.chainage - b.chainage > 6
					group by even.chainage, direction, event_name, link_id, even.the_geom
					order by even.chainage, link_id
					
				) foo
				group by chainage order by chainage) a,
				(select chainage, st_setsrid(st_makepoint(lon::real ,lat::real),4326) as the_geom
				from access_valuelaser where lat != 0 and lon != 0) b 
				where a.chainage between  b.chainage -5 and b.chainage+5
			) foo
			group by chainage
		) foo
	) b
	where b.chainage = event_str
	--chainage = event_str // chainage = event_end
'''
cur_step42 = conPG.cursor()
cur_step42.execute(step42)
conPG.commit()

##STEP_5  update ช่วงที่ต้องการค้นหา โดยกรอก ช่วง max min ที่พบค่า lat lon เป็น 0 โดย  ####################################################################################################################################
step5 = '''
update data_suvey a set lat = b.lat , lon = b.lon , the_geom = the_geompoint
	from (  --create table test_ssa2 as
		select ST_LineInterpolatePoint(the_geom, persent) as the_geompoint,
		st_y(ST_LineInterpolatePoint(the_geom, persent)) as lat,
		st_x(ST_LineInterpolatePoint(the_geom, persent)) as lon, *
		from
		(	
			select *, p1/p2 as persent
			from 
			(
				select a.chainage as gps_lost_ch, c_p, meter, a.link_id, a.date,
				min(b.chainage) as chainage_str, max(b.chainage) as chainage_end,
				(a.chainage- min(b.chainage))::real p1, (max(b.chainage) -min(b.chainage))::real as p2,
				--(a.chainage- min(b.chainage))::real/ (max(b.chainage) -min(b.chainage))::real as persent,
				ST_MakeLine(the_geom ORDER BY b.chainage) as the_geom, file_name
				from gps_lost a, data_suvey b
				where b.chainage between new_p_min and new_p_max and lon != 0
				and st_y(the_geom) > 0  and status = 'มีGPSlost' 
				group by gps_lost_ch, c_p, meter, a.link_id, a.date, file_name
			) foo
			where p2 > 0
		) foo ) b
	where a.file_name = b.file_name and a.date = b.date and a.link_id = b.link_id and a.chainage = b.gps_lost_ch 
'''
cur_step5 = conPG.cursor()
cur_step5.execute(step5)
conPG.commit()

# ##STEP_6  update GPS lost ที่ได้จากการแก้ไขทีมสนาม  ####################################################################################################################################
# step6 = '''
# update data_suvey a set the_geom = b.the_geom, lon = lon_l, lat = lat_l
# 	from
# 	(
# 		SELECT link_id, chainage, (st_dump(st_setsrid(a.geom,4326))).geom as the_geom, file_name,
# 		st_x(a.geom) as lon_l, st_y(a.geom) as lat_l
# 		FROM data_survey_vesion1 a
# 	) b
# where a.chainage = b.chainage and a.link_id = b.link_id and a.file_name = b.file_name
# '''
# cur_step6 = conPG.cursor()
# cur_step6.execute(step6)
# conPG.commit()
print('')
##STEP_7  create survey  ####################################################################################################################################
s_id = str(input('select max(survey_id) from survey ='))
step7 = '''
create table survey as --delete from survey_local
	select
	row_number() over (order by chainage_str::int,link_id)+%s as survey_id, null::int as subsection_id, section_id, link_id,
	left(right(link_id,4),2)||left(link_id,11) as survey_code, 	---'CC41603470101'
	(right(link_id,2)::int)::character(25) as run_code, 
	case 	when left(right(link_id,6),2) = 'L1' then 1
		when left(right(link_id,6),2) = 'L2' then 2
		when left(right(link_id,6),2) = 'L3' then 3
		when left(right(link_id,6),2) = 'L4' then 4
		when left(right(link_id,6),2) = 'L5' then 5
		when left(right(link_id,6),2) = 'L6' then 6
		when left(right(link_id,6),2) = 'R1' then -1
		when left(right(link_id,6),2) = 'R2' then -2
		when left(right(link_id,6),2) = 'R3' then -3
		when left(right(link_id,6),2) = 'R4' then -4
		when left(right(link_id,6),2) = 'R5' then -5
		when left(right(link_id,6),2) = 'R6' then -6
	else 0	end lane_group, 						---lane_group ==> 1=L , -1=R, 2=FL, -2=FR, 3=IL, -3 = IR, 4 = UL, -4 = UR, 5=BL,  -5 = BR, 6 = TL, -6 = TR
	right(lane_no,1)::int as lane_no, null::int as lane_reverse, km_start, km_end, 
	case when left(left(right(link_id,6),2),1) = 'L' then (km_end-km_start)::int
		when left(left(right(link_id,6),2),1) = 'R' then (km_start-km_end)::int end length, 
	(st_length(the_geom::geography)/1000)::numeric(8,3) as distance_odo, (st_length(the_geom::geography)/1000)::numeric(8,3) as distance_gps, left(date::text,4)::int as year,
	case	when left(right(link_id,4),2) = 'AC' then 2
		when left(right(link_id,4),2) = 'CC' then 1
	 end survey_type, 						---1= CC , 2 =AC
	date, the_geom, 'CU_survey'::character(10) as remark, 'S21'::character(10) as run_new, 25::int as interval
	from
	(
		select min(chainage) as chainage_str, max(chainage) as chainage_end, event_name, link_id, section_id::int, 
		replace(km_start, '+', '')::int as km_start, replace(km_end, '+', '')::int as km_end, (length::real)*1000 as length, 
		case when right(lane_no,1) = 'L' then lane_no||'2'
		     when right(lane_no,1) = 'R' then lane_no||'2' else lane_no end lane_no, date,
		st_setsrid(ST_MakeLine(the_geom ORDER BY chainage),4326) AS the_geom, file_name
		from
		(	
			--เชคจุดเส้น create table test_point_survey as 
			select *
			from 
			( --sum event
				select event_str as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name
				from data_suvey a
				group by event_str, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
				st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
				union
				select event_end as chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date,
				st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name
				from data_suvey a
				group by event_end, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, 
				st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
			) even
			union
			select chainage, event_name, link_id, section_id, km_start, km_end, length, lane_no, date, the_geom, file_name
			from data_suvey
			where chainage between event_str and event_end and (st_x(the_geom) > 0 or st_y(the_geom) > 0)
			and split_part((chainage/ CAST(25 AS float))::text, '.', 2) = ''
		) foo
		where link_id != 'construction' and replace(length, ' ', '')::real > 0
		group by event_name, link_id, section_id, km_start, km_end, length, lane_no, date, file_name
	) foo''' % (s_id)
cur_step7 = conPG.cursor()
cur_step7.execute(step7)
conPG.commit()

##STEP_8  create survey_point_local  ####################################################################################################################################
s_point_id = str(input('select max(survey_point_id) from survey_point ='))
step8 = '''
create table survey_point_local as
	select  row_number() over (order by km::int)+%s as survey_point_id, *
	from 
	(
		select row_number() over (partition by a.date, a.link_id order by  a.chainage)-1 as order_row, 
		case 
		when lane_group > 0 then ((row_number() over (partition by a.date, survey_id, a.link_id order by  a.chainage)-1)*25)+km_start 
		when lane_group < 0 then km_start-((row_number() over (partition by a.date, survey_id, a.link_id order by  a.chainage)-1)*25)
		end km,
		a.*, survey_id, survey_code, run_code
		from 
		(
			select *
			from 
			(
				select even.chainage, even.link_id, even.date, even.the_geom, even.file_name,
				avg(iri_right::real)::numeric(8,2) as iri_right, 
				avg(iri_left::real)::numeric(8,2) as iri_left,
				avg(iri::real)::numeric(8,2) as iri,
				avg(iri_lane::real)::numeric(8,2) as iri_lane,
				avg(rutt_right::real)::numeric(8,2) as rutt_right,
				avg(rutt_left::real)::numeric(8,2) as rutt_left,
				avg(rutting::real)::numeric(8,2) as rutting,
				avg(texture::real)::numeric(8,2) as texture,
				avg(etd_texture::real)::numeric(8,2) as etd_texture
				from 
				(
					select event_str as chainage, link_id, date,
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326) as the_geom, file_name
					from data_suvey a
					group by event_str, link_id, date, 
					st_setsrid(st_makepoint(lon_str::real ,lat_str::real),4326), file_name
					union
					select event_end as chainage, link_id, date,
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326) as the_geom, file_name
					from data_suvey a
					group by event_end, link_id, date, 
					st_setsrid(st_makepoint(lon_end::real ,lat_end::real),4326), file_name
				)  even
				left join 
				(
					select 	chainage ,case when event_str - chainage > 0 then event_str
								when chainage - event_str < 5 then event_str
								when event_end - chainage > 0 then event_end
								when chainage - event_end < 5 then event_end
							end chainages, link_id, date, the_geom, file_name,
						iri_right, iri_left, iri, iri_lane, rutt_right, rutt_left, rutting, texture, etd_texture	
					from data_suvey
					where event_str between chainage-4 and chainage+4 or event_end between chainage-4 and chainage+4
				) b on  even.chainage = b.chainages and even.file_name = b.file_name and even.link_id = b.link_id and even.date = b.date
				group by even.chainage, even.link_id, even.date, even.the_geom, even.file_name
			) a

			union

			select b.chainage, a.link_id, a.date, the_geom, a.file_name, a.iri_right, a.iri_left, a.iri,
			a.iri_lane, a.rutt_right, a.rutt_left, a.rutting, a.texture, a.etd_texture
			from 
			(
				select (chainage/25)::int as chainage_s, link_id, date, file_name,
					avg(iri_right::real)::numeric(8,2) as iri_right, 
					avg(iri_left::real)::numeric(8,2) as iri_left,
					avg(iri::real)::numeric(8,2) as iri,
					avg(iri_lane::real)::numeric(8,2) as iri_lane,
					avg(rutt_right::real)::numeric(8,2) as rutt_right,
					avg(rutt_left::real)::numeric(8,2) as rutt_left,
					avg(rutting::real)::numeric(8,2) as rutting,
					avg(texture::real)::numeric(8,2) as texture,
					avg(etd_texture::real)::numeric(8,2) as etd_texture
				from data_suvey
				where chainage between event_str and event_end 
				group by (chainage/25)::int , link_id, date, file_name
				order by chainage_s
			) a
			left join data_suvey b on a.chainage_s*25 = b.chainage
			where the_geom is not null
		) a
		left join survey b on a.link_id = b.link_id
	) foo''' % (s_point_id)
cur_step8 = conPG.cursor()
cur_step8.execute(step8)
conPG.commit()

##STEP_9  create survey_point  ####################################################################################################################################
step9 = '''create table survey_point as 
select survey_point_id, survey_id, km, iri_right, iri_left, iri, iri_lane, 
	rutt_right, rutt_left, rutting, texture, etd_texture, the_geom, left(link_id,3)||'_'||left(date::text,4) as remark
from survey_point_local'''
cur_step9 = conPG.cursor()
cur_step9.execute(step9)
conPG.commit()

##STEP_10  create survey_image  ####################################################################################################################################
step10 = '''create table survey_image as 
select 's21/cu_survey/'||left(a.link_id,3)||'/'||date||'/'||survey_code||'/Run'||run_code||'/image/'||filename as directory,
filename, date, a.chainage as img_id,km, 'True'::text as imagepath, the_geom, survey_id, 'CU_S21'::character(10) as remark 
from
(
	select survey_point_id, survey_id, km, a.link_id, a.chainage, a.file_name, frame_number, a.date, survey_code, run_code,
	a.file_name||'-ROW-0-'||case when length(frame_number::text) = 1 then '0000'||frame_number
				when length(frame_number::text) = 2 then '000'||frame_number::text
				when length(frame_number::text) = 3 then '00'||frame_number::text
				when length(frame_number::text) = 4 then '0'||frame_number::text
				when length(frame_number::text) > 4 then frame_number::text end||'.jpg' as filename,
	a.the_geom
	from survey_point_local a
	left join data_suvey b on case when (right(((a.chainage::int)::text), 1))::int = 6 then (a.chainage::int)+4
				       when (right(((a.chainage::int)::text), 1))::int = 7 then (a.chainage::int)+3
				       when (right(((a.chainage::int)::text), 1))::int = 8 then (a.chainage::int)+2
				       when (right(((a.chainage::int)::text), 1))::int = 9 then (a.chainage::int)+1
				       when (right(((a.chainage::int)::text), 1))::int = 0 then (a.chainage::int)+0
				       when (right(((a.chainage::int)::text), 1))::int = 4 then (a.chainage::int)+1
				       when (right(((a.chainage::int)::text), 1))::int = 3 then (a.chainage::int)+2
				       when (right(((a.chainage::int)::text), 1))::int = 2 then (a.chainage::int)+3
				       when (right(((a.chainage::int)::text), 1))::int = 1 then (a.chainage::int)+4  
				       when (right(((a.chainage::int)::text), 1))::int = 5 then (a.chainage::int)+0 else a.chainage::int end  = chainage_pic::int
	--where survey_id = 1263740085
	group by survey_point_id, survey_id, km, a.link_id, a.chainage, a.file_name, frame_number, a.date, survey_code, run_code,
	filename, a.the_geom
	order by km--survey_point_id
) a'''
cur_step10 = conPG.cursor()
cur_step10.execute(step10)
conPG.commit()
print('step : create survey,survey_point,survey_image successfully')
##STEP_11  Dump SQL  ####################################################################################################################################
dump1 = '''COPY (
SELECT dump('public', 'survey','true')
) TO 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\survey.sql'; ''' % (ymd, run)
cur_dump1 = conPG.cursor()
cur_dump1.execute(dump1)
conPG.commit()

dump2 = '''COPY (
SELECT dump('public', 'survey_point','true')
) TO 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\survey_point.sql'; ''' % (ymd, run)
cur_dump2 = conPG.cursor()
cur_dump2.execute(dump2)
conPG.commit()

dump3 = '''COPY (
SELECT dump('public', 'survey_image','true')
) TO 'D:\\infraplus\\S21\\survey_data\\2021-02-%s\\run%s\\survey_image.sql'; ''' % (ymd, run)
cur_dump3 = conPG.cursor()
cur_dump3.execute(dump3)
conPG.commit()
print('step : Dump SQL successfully')

##STEP_12  .bat file  ####################################################################################################################################
