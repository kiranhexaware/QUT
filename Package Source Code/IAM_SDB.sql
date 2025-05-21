CREATE OR REPLACE PACKAGE IAM_SDB
AS

  SUBTYPE trs_client_id_t IS sdb_client_attributes.client_id%TYPE;

  TYPE trs_client_id_tt IS TABLE OF trs_client_id_t;

  PROCEDURE integrate_client_details(p_trs_client_id  trs_client_id_t);

  PROCEDURE main_control;

END IAM_SDB;
/


CREATE OR REPLACE PACKAGE BODY IAM_SDB
AS

-- Deployed on: Tue Jun  5 18:16:23 EST 2012
-- Deployed from: maple.qut.edu.au:/home/integsvc/novoP/hub/IAM_SDB/tags/1.0.1/iam_sdb.pkb


  gc_interface_name     CONSTANT VARCHAR2 (20) := 'IAM_SDB';

  PROCEDURE integrate_client_details(p_trs_client_id  trs_client_id_t)
  IS
    l_key_data            VARCHAR2(100);
    c_proc_name           CONSTANT VARCHAR2(100) := 'INTEGRATE_CLIENT_DETAILS';
    l_username            sdb_client_attributes.attr_data%TYPE;
    l_email               sdb_client_attributes.attr_data%TYPE;
  BEGIN
    l_key_data := to_char(p_trs_client_id);

    -- get data from source
    SELECT DISTINCT access_name,
                    email_alias||'@'||email_domain
    INTO l_username,
          l_email
    FROM cds_client_role
    WHERE trs_client_id = p_trs_client_id; -- clients may have multiple roles for 1 trs_client_id

    -- upsert access account
    BEGIN

      UPDATE sdb_client_attributes
        set attr_data = l_username
        WHERE client_id = p_trs_client_id
        AND attr_type = 'ACCOUNT'
        AND attr_id = 'ACCESS';

      IF SQL%NOTFOUND THEN
        INSERT INTO sdb_client_attributes (client_id, attr_type, attr_id, attr_data, start_date)
        VALUES (p_trs_client_id, 'ACCOUNT', 'ACCESS', l_username, sysdate);
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'ERROR',
                        NULL,
                        l_key_data,
                        'Error updating Access Account: '||SQLERRM);
    END;

    -- upsert email address
    BEGIN
      IF l_email IS NULL OR l_email = '@' THEN
        l_email := '{NO EMAIL ADDRESS}';
      END IF;

      UPDATE sdb_client_attributes
        set attr_data = l_email
        WHERE client_id = p_trs_client_id
        AND attr_type = 'ACCOUNT'
        AND attr_id = 'EMAIL';

      IF SQL%ROWCOUNT = 0 THEN
        INSERT INTO sdb_client_attributes (client_id, attr_type, attr_id, attr_data, start_date)
        VALUES (p_trs_client_id, 'ACCOUNT', 'EMAIL', l_email, sysdate);
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
        HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'ERROR',
                        NULL,
                        l_key_data,
                        'Error updating Email Account: '||SQLERRM);
    END;

  EXCEPTION
    WHEN OTHERS THEN
      HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'ERROR',
                        NULL,
                        l_key_data,
                        'Error updating details: '||SQLERRM);
  END;


  PROCEDURE main_control
  IS
    c_proc_name           CONSTANT VARCHAR2(100) := 'MAIN_CONTROL';
    l_phase               VARCHAR2(100) := 'Initialising';
    l_log_start           TIMESTAMP;
    l_log_end             TIMESTAMP;
    l_run_start           TIMESTAMP;
    l_run_end             TIMESTAMP;
    l_elapsed_time        INTERVAL DAY (2) TO SECOND (6);
    l_commit_batch_size   NUMBER := 10000;

    l_trs_client_ids      trs_client_id_tt;

  BEGIN
    l_log_start := localtimestamp;
    HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'INFO',
                        NULL,
                        'Starting ' || gc_interface_name,
                        'START AT: ' || TO_CHAR (l_log_start));

    BEGIN
      l_phase := 'Collecting batch';
      SELECT DISTINCT client_id
        BULK COLLECT INTO l_trs_client_ids
        FROM sdb_client_attributes
        WHERE attr_type = 'ROLE'
        AND attr_id IN ('STAFF','STUDENT','CCR');

      l_phase := 'Integrating batch';
      IF l_trs_client_ids.count > 0 THEN
        FOR i IN l_trs_client_ids.first .. l_trs_client_ids.last
        LOOP
          integrate_client_details(l_trs_client_ids(i));

          IF MOD(i, l_commit_batch_size) = 0 THEN
            COMMIT;
            HUB_LOG.LOG_WRITE (gc_interface_name,
                            c_proc_name,
                            'INFO',
                            NULL,
                            'i = '||i,
                            'Commit point at record number '||i);
          END IF;

        END LOOP;

        COMMIT;
      END IF;

    EXCEPTION
      WHEN OTHERS THEN
          HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'ERROR',
                        NULL,
                        l_phase,
                        'MAIN control ERROR: '||SQLERRM);
    END;

    l_log_end := localtimestamp;
    l_elapsed_time := l_log_end - l_log_start;
    HUB_LOG.LOG_WRITE (gc_interface_name,
                        c_proc_name,
                        'INFO',
                        NULL,
                        'Elapsed TIME ' || l_elapsed_time,
                        'Ended AT: ' || TO_CHAR (l_log_end));
  END;

END IAM_SDB;
/
