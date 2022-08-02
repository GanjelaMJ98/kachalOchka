CREATE SCHEMA smartc;

CREATE TABLE smartc.meetings (
	meeting_id                  UUID,
	domain                      TEXT,
	importance_type             TEXT,
	members_cnt                 INT,
	subject                     TEXT,
	meeting_place               TEXT,
	meeting_initiator           TEXT,
	init_fio                    TEXT,
	init_legacyexchangedn_flg   INT,
	member                      TEXT,
	member_fio                  TEXT,
	member_legacyexchangedn_flg INT,
	member_status               TEXT,
	member_responce_status      TEXT,
	meeting_start_dttm          TIMESTAMP,
	meeting_end_dttm            TIMESTAMP,
	version                     INT,
	tabnum                      INT,
	initiator_tabnum            INT
);

DROP TABLE IF EXISTS smartc.test_table;
CREATE TABLE smartc.test_table AS
    WITH CLEAR_SOURCE AS (
                SELECT meeting_id,domain,importance_type,members_cnt,subject,meeting_initiator,init_fio,init_legacyexchangedn_flg,
                       member,member_fio,member_legacyexchangedn_flg,member_status,member_responce_status,meeting_start_dttm,meeting_end_dttm,version,
                       COALESCE(meeting_place,'') as meeting_place, COALESCE(tabnum,0) as tabnum, COALESCE(initiator_tabnum,0) as initiator_tabnum
                FROM smartc.meetings
                WHERE meeting_start_dttm IS NOT NULL AND meeting_end_dttm IS NOT NULL AND subject IS NOT NULL AND subject != '' AND
                      member IS NOT NULL AND member != '' AND meeting_initiator IS NOT NULL AND meeting_initiator != '' AND
                      member NOT LIKE '%/o%' AND meeting_initiator NOT LIKE '%/o%' AND member_responce_status != 'decline' ),
        MEETINGS_WITH_INITIATORS AS (
                SELECT meeting_id, meeting_start_dttm, meeting_end_dttm, subject, meeting_place,
                       meeting_initiator,member, domain, tabnum, initiator_tabnum
                FROM CLEAR_SOURCE
                UNION
                SELECT DISTINCT ON (meeting_id, meeting_start_dttm, subject)
                       meeting_id, meeting_start_dttm, meeting_end_dttm, subject, meeting_place,meeting_initiator,
                       meeting_initiator as member, domain, initiator_tabnum as tabnum, initiator_tabnum
                FROM CLEAR_SOURCE ),
        MEETINGS_WITH_TABNUM_ARRAY AS (
                SELECT meeting_id, meeting_start_dttm, meeting_end_dttm, subject, meeting_place, domain,
                       meeting_initiator, member, tabnum, initiator_tabnum,
                       array_agg(tabnum) OVER (PARTITION BY meeting_id, meeting_start_dttm, subject) as partitipants_tabnums,
                       array_agg(member) OVER (PARTITION BY meeting_id, meeting_start_dttm, subject) as partitipants_emails,
                       CASE WHEN subject LIKE '%Canceled%' OR subject LIKE '%Отменено%' THEN 1 ELSE 0 END canceled
                FROM MEETINGS_WITH_INITIATORS ),
        USER_MEETINGS AS (
                SELECT DISTINCT ON (meeting_id, meeting_start_dttm, meeting_end_dttm, meeting_place, tabnum, initiator_tabnum) *
                FROM MEETINGS_WITH_TABNUM_ARRAY )
SELECT *, array_length(partitipants_tabnums, 1) AS cnt FROM USER_MEETINGS;

SELECT * FROM smartc.test_table WHERE tabnum = 5555
