CREATE OR REPLACE PACKAGE QV_IAM
AS

-- Deployed on: Mon Apr  7 15:12:08 EST 2014
-- Deployed from: intdeploy.qut.edu.au:/home/integsvc/novoP/hub/QV_IAM/tags/1.0.4/QV_IAM.pks

/**
* Optimised version of QV_IAM interface to use SOA design
*
* QV_IAM package to integrate phonebook data from QV into IAM webservice
* Replaces existing QV -> flat file -> eDir integration
* Uses sql queries from previous integration solution
* Candidate for simplification, and migration into Novo integration
*
* This package does not support deletes - as per its predecessor.
* Assumption - that when a person leaves QUT, their AD/IAM root records will
* be expired, and all child records will also expire - meaning that a delete
* is not required.
*/

  PROCEDURE SYNC_CLIENT_CONTACT (P_ACCOUNTID qv_client_role.trs_client_id%TYPE);

  PROCEDURE MAIN_CONTROL;

END QV_IAM;
/


CREATE OR REPLACE PACKAGE BODY QV_IAM
AS

-- Deployed on: Mon Apr  7 15:12:08 EST 2014
-- Deployed from: intdeploy.qut.edu.au:/home/integsvc/novoP/hub/QV_IAM/tags/1.0.4/QV_IAM.pkb

  GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'QV_IAM';
  G_RUN_START                  TIMESTAMP;
  G_RUN_END                    TIMESTAMP;
  G_MOVE_DATE_WINDOW           BOOLEAN;


  -- Code mostly copied from novo_restful_ws_tst
  PROCEDURE LOAD_IAM (p_accountid       qv_client_role.trs_client_id%TYPE,
                      p_phone           ip.primary_extn%TYPE,
                      p_mobile          ip.mobile%TYPE,
                      p_fax             ip.primary_fax%TYPE,
                      p_speeddial       ip.speed_dial%TYPE,
                      p_campus          locn_site.NAME%TYPE,
                      p_building        locn_building.NAME%TYPE,
                      p_floor           locn_floor.NAME%TYPE,
                      p_room            locn_location.room_id%TYPE,
                      p_notes           ip.location_notes%TYPE,
		              p_building_id     locn_location.building_id%TYPE,
	                  p_floor_id        locn_location.floor_id%TYPE
					  )
  IS

    C_THIS_PROC   CONSTANT VARCHAR2(20) := 'LOAD_IAM';

  BEGIN

    QV_PHONEBOOK_UTIL.set_phonebook_address (p_accountid
                                           , p_phone
                                           , p_mobile
                                           , p_fax
                                           , p_speeddial
                                           , p_campus
                                           , p_building
                                           , p_floor
                                           , p_room
                                           , p_notes
										   , p_building_id
										   , p_floor_id
										   , null -- would be sending workstation_id in future
										   );

  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'LOAD IAM EXCEPTION - ACCOUNT ID - ' || p_accountid, SQLERRM);
      G_MOVE_DATE_WINDOW := FALSE;

  END LOAD_IAM;


  PROCEDURE STAFF_CONTACTS_ETL (
      P_ACCOUNTID qv_client_role.trs_client_id%TYPE)
  IS

    C_THIS_PROC   CONSTANT VARCHAR2(20) := 'STAFF_CONTACTS_ETL';

    -- Cursor SQL copied directly from existing integration.
    -- Small modification to extract location data out of locn_* tables
    -- instead of primary_location field.
    CURSOR STAFF_CONTACTS_EXTRACT(P_CLIENT_ID qv_client_role.trs_client_id%TYPE)
    IS
      SELECT trs_client_id AS accountid, A.primary_extn AS phone, mobile
      , A.primary_fax AS fax, speed_dial AS speeddial, ls.NAME AS campus
      , lb.NAME AS building, lf.NAME AS floor, ll.room_id AS room
      , location_notes AS notes,ll.building_id as building_id, ll.floor_id as floor_id
      FROM   group_codes    gc,
             subgroup_codes sc,
             emp_org_unit   c4,
             emp_org_unit   c3,
             emp_org_unit   c2,
             ip              A,
             qv_client_computer_account qca,
             qv_client_role             r,
             locn_location  ll,
             locn_site      ls,
             locn_building  lb,
             locn_floor     lf
      WHERE (A.phone_group    = gc.phone_group (+)
      AND    A.owner_org_code = gc.owner_org_code (+))
      AND   (A.phone_subgroup = sc.phone_subgroup (+)
      AND    A.phone_group    = sc.phone_group (+)
      AND    A.owner_org_code = sc.owner_org_code (+))
      AND   (c4.org_unit_cd   = A.owner_org_code
      AND    c4.hierarchy_level= 'CLEVEL4')
      AND   (c3.org_unit_cd   = substr(A.owner_org_code,1,5)
      AND    c3.hierarchy_level= 'CLEVEL3')
      AND   (c2.org_unit_cd   = substr(A.owner_org_code,1,3)
      AND    c2.hierarchy_level= 'CLEVEL2')
      AND  ((SYSDATE BETWEEN c2.start_dt AND c2.end_dt) OR c2.end_dt IS NULL)
      AND  ((SYSDATE BETWEEN c3.start_dt AND c3.end_dt) OR c3.end_dt IS NULL)
      AND  ((SYSDATE BETWEEN c4.start_dt AND c4.end_dt) OR c4.end_dt IS NULL)
      AND   r.ID              = A.employee_num
      AND   r.role_cd         = 'EMP'
      AND   r.role_active_ind = 'Y'
      AND   r.username        = qca.username
      AND   A.ip_status = 'cur'
      AND   A.location_id     = ll.location_id (+)
      AND   ll.site_id        = ls.site_id (+)
      AND   ll.site_id        = lb.site_id (+)
      AND   ll.building_id    = lb.building_id (+)
      AND   ll.building_id    = lf.building_id (+)
      AND   ll.floor_id       = lf.floor_id (+)
      AND   r.trs_client_id   = P_CLIENT_ID;

  BEGIN

    FOR PHONEBOOK_REC IN STAFF_CONTACTS_EXTRACT(P_ACCOUNTID)
    LOOP
      LOAD_IAM (PHONEBOOK_REC.ACCOUNTID,
                PHONEBOOK_REC.PHONE,
                PHONEBOOK_REC.MOBILE,
                PHONEBOOK_REC.FAX,
                PHONEBOOK_REC.SPEEDDIAL,
                PHONEBOOK_REC.CAMPUS,
                PHONEBOOK_REC.BUILDING,
                PHONEBOOK_REC.FLOOR,
                PHONEBOOK_REC.ROOM,
                PHONEBOOK_REC.NOTES,
				PHONEBOOK_REC.BUILDING_ID,
				PHONEBOOK_REC.FLOOR_ID
				);

    END LOOP;

  END STAFF_CONTACTS_ETL;


    PROCEDURE VISITOR_CONTACTS_ETL (
      P_ACCOUNTID qv_client_role.trs_client_id%TYPE)
  IS

    C_THIS_PROC   CONSTANT VARCHAR2(20) := 'VISITOR_CONTACTS_ETL';

    -- Cursor SQL copied directly from existing integration.
    -- Small modification to extract location data out of locn_* tables
    -- instead of primary_location field.
    CURSOR VISITOR_CONTACTS_EXTRACT(P_CLIENT_ID qv_client_role.trs_client_id%TYPE)
    IS
      SELECT trs_client_id AS accountid, A.primary_extn AS phone, mobile
      , A.primary_fax AS fax, speed_dial AS speeddial, ls.NAME AS campus
      , lb.NAME AS building, lf.NAME AS floor, ll.room_id AS room
      , location_notes AS notes,ll.building_id as building_id, ll.floor_id as floor_id
      FROM   group_codes    gc,
             subgroup_codes sc,
             emp_org_unit   c4,
             emp_org_unit   c3,
             emp_org_unit   c2,
             ip              A,
             qv_client_computer_account qca,
             qv_client_role             r,
             ccr_clients                cc,
             locn_location ll,
             locn_site ls,
             locn_building lb,
             locn_floor lf
      WHERE (A.phone_group    = gc.phone_group (+)
      AND    A.owner_org_code = gc.owner_org_code (+))
      AND   (A.phone_subgroup = sc.phone_subgroup (+)
      AND    A.phone_group    = sc.phone_group (+)
      AND    A.owner_org_code = sc.owner_org_code (+))
      AND   (c4.org_unit_cd   = A.owner_org_code
      AND    c4.hierarchy_level= 'CLEVEL4')
      AND   (c3.org_unit_cd   = substr(A.owner_org_code,1,5)
      AND    c3.hierarchy_level= 'CLEVEL3')
      AND   (c2.org_unit_cd   = substr(A.owner_org_code,1,3)
      AND    c2.hierarchy_level= 'CLEVEL2')
      AND  ((SYSDATE BETWEEN c2.start_dt AND c2.end_dt) OR c2.end_dt IS NULL)
      AND  ((SYSDATE BETWEEN c3.start_dt AND c3.end_dt) OR c3.end_dt IS NULL)
      AND  ((SYSDATE BETWEEN c4.start_dt AND c4.end_dt) OR c4.end_dt IS NULL)
      AND   r.role_cd         = 'CCR'
      AND   r.role_active_ind = 'Y'
      AND   r.username        = qca.username
      AND   r.ID              = cc.ccr_client_id
      AND   A.ip_num          = cc.ip_num
      AND   cc.deceased_flag  = 'N'
      AND   A.ip_status = 'cur'
      AND   A.location_id     = ll.location_id (+)
      AND   ll.site_id        = ls.site_id (+)
      AND   ll.site_id        = lb.site_id (+)
      AND   ll.building_id    = lb.building_id (+)
      AND   ll.building_id    = lf.building_id (+)
      AND   ll.floor_id       = lf.floor_id (+)
      AND   r.trs_client_id   = P_CLIENT_ID;

    L_ACCOUNTID       qv_client_role.trs_client_id%TYPE;
    L_PHONE           ip.primary_extn%TYPE;
    L_MOBILE          ip.mobile%TYPE;
    L_FAX             ip.primary_fax%TYPE;
    L_SPEEDDIAL       ip.speed_dial%TYPE;
    L_CAMPUS          locn_site.NAME%TYPE;
    L_BUILDING        locn_building.NAME%TYPE;
    L_FLOOR           locn_floor.NAME%TYPE;
    L_ROOM            locn_location.room_id%TYPE;
    L_NOTES           ip.location_notes%TYPE;
    L_BUILDING_ID     locn_location.building_id%TYPE;
    L_FLOOR_ID        locn_location.floor_id%TYPE;

  BEGIN

    FOR PHONEBOOK_REC IN VISITOR_CONTACTS_EXTRACT(P_ACCOUNTID)
    LOOP
      L_ACCOUNTID := PHONEBOOK_REC.ACCOUNTID;
      L_PHONE     := PHONEBOOK_REC.PHONE;
      L_MOBILE    := PHONEBOOK_REC.MOBILE;
      L_FAX       := PHONEBOOK_REC.FAX;
      L_SPEEDDIAL := PHONEBOOK_REC.SPEEDDIAL;
      L_CAMPUS    := PHONEBOOK_REC.CAMPUS;
      L_BUILDING  := PHONEBOOK_REC.BUILDING;
      L_FLOOR     := PHONEBOOK_REC.FLOOR;
      L_ROOM      := PHONEBOOK_REC.ROOM;
      L_NOTES     := PHONEBOOK_REC.NOTES;
	  L_BUILDING_ID := PHONEBOOK_REC.BUILDING_ID;
      L_FLOOR_ID   :=  PHONEBOOK_REC.FLOOR_ID;

      LOAD_IAM (L_ACCOUNTID,
                L_PHONE,
                L_MOBILE,
                L_FAX,
                L_SPEEDDIAL,
                L_CAMPUS,
                L_BUILDING,
                L_FLOOR,
                L_ROOM,
                L_NOTES,
				L_BUILDING_ID,
				L_FLOOR_ID);

    END LOOP;

  END VISITOR_CONTACTS_ETL;


  PROCEDURE SYNC_CLIENT_CONTACT (P_ACCOUNTID qv_client_role.trs_client_id%TYPE)
  IS
    C_THIS_PROC   CONSTANT VARCHAR2(20) := 'SYNC_CLIENT_CONTACT';
    L_PHASE                VARCHAR2(50) := 'Initialising';

  BEGIN
    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'AccountID - ' || P_ACCOUNTID, 'Manual run of SYNC_CLIENT_CONTACT');

    BEGIN
      L_PHASE := 'UPDATE STAFF CONTACT DETAILS';
      STAFF_CONTACTS_ETL (P_ACCOUNTID);
      L_PHASE := 'UPDATE VISITOR CONTACT DETAILS';
      VISITOR_CONTACTS_ETL (P_ACCOUNTID);

    EXCEPTION
      WHEN OTHERS
      THEN
        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'SYNC_CLIENT_CONTACT WHEN OTHERS EXCEPTION during Phase: ' || L_PHASE, SQLERRM);

    END;

    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'AccountID - ' || P_ACCOUNTID, 'Completed manual run of SYNC_CLIENT_CONTACT');

  END SYNC_CLIENT_CONTACT;


  PROCEDURE MAIN_CONTROL
  IS
    C_THIS_PROC   CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
    L_PHASE                VARCHAR2(50) := 'Initialising';
    L_START_TIME           TIMESTAMP;
    L_END_TIME             TIMESTAMP;
    L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);

    l_from                 TIMESTAMP;
    l_to                   TIMESTAMP;

    l_counter              NUMBER;

    CURSOR contacts_changed_keys (p_from TIMESTAMP, p_to TIMESTAMP)
    IS
      SELECT ip_num
      FROM ip
         , locn_location locn
         , locn_site site
         , locn_building building
         , locn_floor floor
      WHERE ip.location_id = locn.location_id (+)
      AND locn.site_id = site.site_id (+)
      AND locn.site_id = building.site_id (+)
      AND locn.building_id = building.building_id (+)
      AND locn.building_id = floor.building_id (+)
      AND locn.floor_id = floor.floor_id  (+)
      AND -- 01-JAN-1900 used to allow GREATEST function to work correctly
        GREATEST(ip.update_on
           , NVL(locn.update_on, TO_DATE('01-JAN-1900','DD-MON-YYYY'))
           , NVL(site.update_on, TO_DATE('01-JAN-1900','DD-MON-YYYY'))
           , NVL(building.update_on, TO_DATE('01-JAN-1900','DD-MON-YYYY'))
           , NVL(floor.update_on, TO_DATE('01-JAN-1900','DD-MON-YYYY')))
        BETWEEN p_from AND p_to;

  BEGIN
    -- Log that this interface has started.
    L_START_TIME := LOCALTIMESTAMP;
    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Starting ' || GC_INTERFACE_NAME, 'Start at: ' || TO_CHAR(L_START_TIME));

    G_MOVE_DATE_WINDOW := TRUE;

    HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, l_from, l_to);

    BEGIN
      l_counter := 0;

      FOR contact_key_rec IN contacts_changed_keys(l_from, l_to) LOOP

        -- For each record, try and update it as both a staff member and a visitor
        L_PHASE := 'VISITOR_CONTACTS_ETL - ACCOUNT_ID - '|| contact_key_rec.ip_num;
        VISITOR_CONTACTS_ETL (contact_key_rec.ip_num);
        L_PHASE := 'STAFF_CONTACTS_ETL - ACCOUNT_ID - '|| contact_key_rec.ip_num;
        STAFF_CONTACTS_ETL (contact_key_rec.ip_num);

        l_counter := l_counter + 1;

        IF MOD(l_counter, 1000) = 0 THEN -- Heartbeat log every 1000 records processed
          HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'DEBUG', NULL, NULL, 'Records processed: ' || l_counter);

        END IF;

      END LOOP;

      IF l_counter = 0 THEN
        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, NULL, 'No records found within date window');

      ELSE
        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, NULL, 'Total records processed: ' || l_counter);

      END IF;

    EXCEPTION
      WHEN OTHERS
      THEN
        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: ' || L_PHASE, SQLERRM);
        G_MOVE_DATE_WINDOW := FALSE;

    END;

    IF G_MOVE_DATE_WINDOW THEN
      HUB_LIB.set_run_dates (GC_INTERFACE_NAME, l_from, l_to);

    END IF;

    -- Log that this interface has finished.
    L_END_TIME := LOCALTIMESTAMP;
    L_ELAPSED_TIME := L_END_TIME - L_START_TIME;
    HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Elapsed time ' || L_ELAPSED_TIME, 'Ended at: ' || TO_CHAR(L_END_TIME));

  END MAIN_CONTROL;

END QV_IAM;
/
