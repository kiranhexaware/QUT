CREATE OR REPLACE PACKAGE FINANCE_QV
AS
   PROCEDURE MAIN_CONTROL;
END FINANCE_QV;
/


CREATE OR REPLACE PACKAGE BODY FINANCE_QV

-- Deployed on: Mon May 20 13:46:58 EST 2013
-- Deployed from: maple.qut.edu.au:/home/integsvc/novoP/hub/FINANCE_QV/tags/1.2.0/Packages/FINANCE_QV.pkb

AS
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'FINANCE_QV';
   G_RUN_START                  TIMESTAMP;
   G_RUN_END                    TIMESTAMP;

   PROCEDURE DELETE_USERNAMES
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR USER_CURSOR
      IS
         SELECT *
           FROM FND_USER
          WHERE FND_USER.END_DATE < SYSDATE
                AND LENGTH(FND_USER.USER_NAME) <= 8;

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_ACCESS_CD          VARCHAR2(4000);
      VAR_SYSTEM_CD          VARCHAR2(4000);
      VAR_USERNAME           VARCHAR2(4000);
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'DELETE_USERNAMES';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'ACCESS_TYPE_MEMBER';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR USER_REC IN USER_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_STATUS := NULL;
         VAR_ACCESS_CD := '1';
         VAR_SYSTEM_CD := 'FIN_SYS';
         VAR_USERNAME := USER_REC.USER_NAME;
         VAR_KEY_DATA :=
               'ACCESS_CD: '
            || VAR_ACCESS_CD
            || '|'
            || 'SYSTEM_CD: '
            || VAR_SYSTEM_CD
            || '|'
            || 'USERNAME: '
            || VAR_USERNAME;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         BEGIN
			DELETE FROM ACCESS_TYPE_MEMBER
                  WHERE     ACCESS_CD = VAR_ACCESS_CD
                        AND GROUP_CD = VAR_SYSTEM_CD
                        AND USERNAME = VAR_USERNAME;


            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
         EXCEPTION
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, VAR_KEY_DATA, 'RECORD DELETES WHEN OTHERS EXCEPTION: ' || SQLERRM);
               RAISE;
         END;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      -- Unique-per-source
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
         ROLLBACK;
   END DELETE_USERNAMES;

   PROCEDURE INSERT_USERNAMES
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR USER_CURSOR
      IS
         SELECT *
           FROM FND_USER
          WHERE (FND_USER.END_DATE IS NULL OR FND_USER.END_DATE >= SYSDATE)
                AND LENGTH(FND_USER.USER_NAME) <= 8;

      VAR_KEY_DATA           VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_ACCESS_CD          VARCHAR2(4000);
      VAR_ACCESS_TYPE        VARCHAR2(4000);
      VAR_SYSTEM_CD          VARCHAR2(4000);
      VAR_USERNAME           VARCHAR2(4000);

      VAR_REC                VARCHAR2(4000);
      VAR_ERROR              VARCHAR2(4000);
      VAR_ACC                VARCHAR2(4000);
      VAR_ERROR1             VARCHAR2(4000);

      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'INSERT_USERNAMES';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'ACCESS_TYPE_MEMBER';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Transaction Actions
      ------------------------------------------
      FOR USER_REC IN USER_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : Pre Record Actions
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_USERNAME := USER_REC.USER_NAME;
         VAR_REC := NULL;
         VAR_ERROR := NULL;
         VAR_ACC := NULL;
         VAR_ERROR1 := NULL;
         VAR_STATUS := NULL;
         VAR_STATUS := NULL;
         VAR_ACCESS_CD := '1';
         VAR_ACCESS_TYPE := 'USER';
         VAR_SYSTEM_CD := 'FIN_SYS';
         VAR_USERNAME := USER_REC.USER_NAME;

         VAR_KEY_DATA :=
               'ACCESS_CD: '
            || VAR_ACCESS_CD
            || '|'
            || 'SYSTEM_CD: '
            || VAR_SYSTEM_CD
            || '|'
            || 'USERNAME: '
            || VAR_USERNAME;

         BEGIN
            SELECT 'EXISTED'
              INTO VAR_STATUS
              FROM ACCESS_TYPE_MEMBER
             WHERE     USERNAME = VAR_USERNAME
                   AND GROUP_CD = VAR_SYSTEM_CD
                   AND ACCESS_CD = VAR_ACCESS_CD
                   AND ACCESS_TYPE = VAR_ACCESS_TYPE;

         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               NULL;
         END;

         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT 'ACTIVE'
                 INTO VAR_ACC
                 FROM QV_CLIENT_COMPUTER_ACCOUNT
                WHERE USERNAME = VAR_USERNAME AND ACCOUNT_ACTIVE_IND = 'Y';
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  NULL;
            END;

            IF VAR_ACC = 'ACTIVE'
            THEN
               IF LENGTH(VAR_USERNAME) <= 8
               THEN
                  ------------------------------------------
                  -- Constellar Hub : Actions = Insert, Update or Delete
                  ------------------------------------------
                  -- Perform insert, upsert or delete functions
                  BEGIN
					INSERT INTO ACCESS_TYPE_MEMBER(ACCESS_CD, ACCESS_TYPE,
								UPDATE_DT, UPDATE_WHO, GROUP_CD,
								  USERNAME)
						 VALUES (VAR_ACCESS_CD, VAR_ACCESS_TYPE, SYSDATE,
								   'HUB_LINK', VAR_SYSTEM_CD, VAR_USERNAME);

                     HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, SQLERRM);
                  EXCEPTION
                     WHEN OTHERS
                     THEN
                        HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'RECORD INSERTS WHEN OTHERS EXCEPTION', SQLERRM);
                        RAISE;
                  END;
               ELSE
                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, 'USERNAME LENGTH IS GREATER THAN 8');
               END IF;
            ELSE
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, 'ACCOUNT NOT ACTIVE IN QV_CLIENT_COMPUTER_ACCOUNT ');
            END IF;
         ELSE
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, VAR_KEY_DATA, 'RECORD EXISTED ');
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      COMMIT;
   EXCEPTION
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
         ROLLBACK;
   END INSERT_USERNAMES;

   PROCEDURE MAIN_CONTROL
   IS
      C_THIS_PROC   CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
      L_PHASE                VARCHAR2(50) := 'Initialising';
      L_START_TIME           TIMESTAMP;
      L_END_TIME             TIMESTAMP;
      L_ELAPSED_TIME         INTERVAL DAY(2) TO SECOND(6);
   BEGIN
      -- Log that this interface has started.
      L_START_TIME := LOCALTIMESTAMP;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Starting ' || GC_INTERFACE_NAME, 'Start at: ' || TO_CHAR(L_START_TIME));

      BEGIN
         L_PHASE := 'EXECUTE TRANSACTIONS';
         DELETE_USERNAMES;
         INSERT_USERNAMES;
      EXCEPTION
         WHEN OTHERS
         THEN
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: ' || L_PHASE, SQLERRM);
      END;

      -- Log that this interface has finished.
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME := L_END_TIME - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Elapsed time ' || L_ELAPSED_TIME, 'Ended at: ' || TO_CHAR(L_END_TIME));
   END MAIN_CONTROL;
END FINANCE_QV;
/
