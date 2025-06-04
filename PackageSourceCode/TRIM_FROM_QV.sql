CREATE OR REPLACE PACKAGE TRIM_FROM_QV
AS
   PROCEDURE MAIN_CONTROL;
END TRIM_FROM_QV;
/


CREATE OR REPLACE PACKAGE BODY TRIM_FROM_QV
AS
   GC_INTERFACE_NAME   CONSTANT VARCHAR2(20) := 'TRIM_from_QV';
   G_RUN_START                  TIMESTAMP;
   G_RUN_END                    TIMESTAMP;

   PROCEDURE PRE_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Interface Rule
      ------------------------------------------
      --      global_INTERFACE_NAME := NULL;
      --      global_RUNID := NULL;
      --      global_JOBID := NULL;
      --      global_RUN_START := NULL;
      --      global_RUN_END := NULL;

      BEGIN
         HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
      EXCEPTION
         -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
         WHEN OTHERS
         THEN
            HUB_LIB.GET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
      END;
   END PRE_INTERFACE;

   PROCEDURE SET_TRIM_LOCATION_REC(P_ACTION IN VARCHAR2,
      P_TRANSACTION_NAME IN VARCHAR2, P_LOCATION_ID IN VARCHAR2,
      P_ACTIVE_IND IN VARCHAR2, P_CANCEL_DT IN DATE, P_CREATED_DT IN DATE,
      P_DEPTH IN NUMBER, P_DESCRIPTION IN VARCHAR2, P_MODIFIED_DT IN DATE,
      P_PARENT IN VARCHAR2)
   IS
   BEGIN
      IF P_ACTION = 'CREATE'
      THEN
         BEGIN
            INSERT INTO TRIM_LOCATION(LOCATION_ID, ACTIVE_IND, CANCEL_DT,
                           CREATED_DT, DEPTH, DESCRIPTION, MODIFIED_DT,
                           PARENT)
                 VALUES (P_LOCATION_ID, P_ACTIVE_IND, P_CANCEL_DT,
                         P_CREATED_DT, P_DEPTH, P_DESCRIPTION, P_MODIFIED_DT,
                         P_PARENT);

            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'DEBUG', 'TRIM_LOCATION', 'INSERT', P_LOCATION_ID);
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'ERROR', 'TRIM_LOCATION', 'RECORD INSERT WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      ELSE
         BEGIN
            UPDATE TRIM_LOCATION
               SET LOCATION_ID = P_LOCATION_ID, ACTIVE_IND = P_ACTIVE_IND,
                   CANCEL_DT = P_CANCEL_DT, CREATED_DT = P_CREATED_DT,
                   DEPTH = P_DEPTH, DESCRIPTION = P_DESCRIPTION,
                   MODIFIED_DT = P_MODIFIED_DT, PARENT = P_PARENT
             WHERE LOCATION_ID = P_LOCATION_ID;

            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'DEBUG', 'TRIM_LOCATION', 'UPDATE', P_LOCATION_ID);
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, P_TRANSACTION_NAME, 'ERROR', 'TRIM_LOCATION', 'RECORD UPDATE WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END IF;
   END SET_TRIM_LOCATION_REC;

   PROCEDURE TRIM_SITE_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR LOCATION_CURSOR
      IS
         SELECT LOC.SITE_ID, NAME, ACTIVE_IND, LOCATION_ID, BUILDING_ID,
                FLOOR_ID, ROOM_ID
           FROM LOCN_SITE SITE, LOCN_LOCATION LOC
          WHERE     LOC.SITE_ID = SITE.SITE_ID
                AND LOC.BUILDING_ID IS NULL
                AND LOC.FLOOR_ID IS NULL
                AND LOC.ROOM_ID IS NULL;

      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_SITE_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_LOCATION';
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR LOCATION_REC IN LOCATION_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_DESCRIPTION := LOCATION_REC.NAME;
         VAR_STATUS := NULL;

         -- Check if the qv location existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_LOCATION
             WHERE LOCATION_ID = LOCATION_REC.LOCATION_ID;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Check if the record existed and same in Trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_LOCATION
                WHERE     LOCATION_ID = LOCATION_REC.LOCATION_ID
                      AND NVL(PARENT, '0') = '0'
                      AND DESCRIPTION = LOCATION_REC.NAME
                      AND ACTIVE_IND = LOCATION_REC.ACTIVE_IND;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_STATUS := 'MODIFIED';
            END;
         END IF;

         IF VAR_STATUS = 'MODIFIED'
         THEN
            VAR_MODIFIED_DT := SYSDATE;
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, SYSDATE, '1', VAR_DESCRIPTION, VAR_MODIFIED_DT, NULL);
         ELSE
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, VAR_CREATED_DT, '1', VAR_DESCRIPTION, VAR_MODIFIED_DT, NULL);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT LOCATION_ID
                 FROM TRIM_LOCATION
                WHERE PARENT IS NULL AND CANCEL_DT IS NULL
               MINUS
               SELECT LOCATION_ID
                 FROM LOCN_LOCATION, LOCN_SITE
                WHERE     LOCN_LOCATION.SITE_ID = LOCN_SITE.SITE_ID
                      AND LOCN_LOCATION.BUILDING_ID IS NULL
                      AND LOCN_LOCATION.FLOOR_ID IS NULL
                      AND LOCN_LOCATION.ROOM_ID IS NULL;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_LOCATION T
                     SET T.CANCEL_DT = SYSDATE
                   WHERE T.LOCATION_ID = ROW.LOCATION_ID;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.LOCATION_ID || '|' || SYSDATE);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_SITE_IU;

   PROCEDURE TRIM_BUILDING_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR LOCATION_CURSOR
      IS
         SELECT LOC.LOCATION_ID, LOC1.LOCATION_ID AS PARENT,
                BUILDING.ACTIVE_IND, BUILDING.NAME, SITE.SITE_ID
           FROM LOCN_LOCATION LOC, LOCN_SITE SITE, LOCN_BUILDING BUILDING,
                LOCN_LOCATION LOC1
          WHERE     LOC.SITE_ID = SITE.SITE_ID
                AND LOC.BUILDING_ID = BUILDING.BUILDING_ID
                AND LOC.FLOOR_ID IS NULL
                AND LOC.ROOM_ID IS NULL
                AND LOC.SITE_ID =
                          LOC1.SITE_ID
                       || LOC1.BUILDING_ID
                       || LOC1.FLOOR_ID
                       || LOC1.ROOM_ID;

      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_BUILDING_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_LOCATION';
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_EXIST              VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR LOCATION_REC IN LOCATION_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         VAR_DESCRIPTION := LOCATION_REC.SITE_ID || ' ' || LOCATION_REC.NAME;
         VAR_STATUS := NULL;
         VAR_EXIST := NULL;

         -- Check if the qv location existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_LOCATION
             WHERE LOCATION_ID = LOCATION_REC.LOCATION_ID;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Check if the record existed and same in Trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_LOCATION
                WHERE     LOCATION_ID = LOCATION_REC.LOCATION_ID
                      AND NVL(PARENT, '0') = NVL(LOCATION_REC.PARENT, '0')
                      AND DESCRIPTION = VAR_DESCRIPTION
                      AND ACTIVE_IND = LOCATION_REC.ACTIVE_IND;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_STATUS := 'MODIFIED';
            END;
         END IF;

         IF VAR_STATUS = 'MODIFIED'
         THEN
            VAR_MODIFIED_DT := SYSDATE;
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- PERFORM INSERT, UPSERT OR DELETE FUNCTIONS
         IF VAR_STATUS = 'CREATE'
         THEN
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, SYSDATE, '2', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         ELSE
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, VAR_CREATED_DT, '2', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT LOCATION_ID
                 FROM TRIM_LOCATION
                WHERE DEPTH = '2' AND CANCEL_DT IS NULL
               MINUS
               SELECT LOCN_LOCATION.LOCATION_ID
                 FROM LOCN_LOCATION, LOCN_SITE, LOCN_LOCATION LOCN_B,
                      LOCN_BUILDING
                WHERE LOCN_LOCATION.SITE_ID = LOCN_SITE.SITE_ID
                      AND LOCN_LOCATION.BUILDING_ID =
                             LOCN_BUILDING.BUILDING_ID
                      AND LOCN_LOCATION.SITE_ID =
                                LOCN_B.SITE_ID
                             || LOCN_B.BUILDING_ID
                             || LOCN_B.FLOOR_ID
                             || LOCN_B.ROOM_ID
                      AND LOCN_LOCATION.FLOOR_ID IS NULL
                      AND LOCN_LOCATION.ROOM_ID IS NULL;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_LOCATION T
                     SET T.CANCEL_DT = SYSDATE
                   WHERE T.LOCATION_ID = ROW.LOCATION_ID;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.LOCATION_ID);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_BUILDING_IU;


   PROCEDURE TRIM_FLOOR_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR LOCATION_CURSOR
      IS
         SELECT LOC.LOCATION_ID, LOC1.LOCATION_ID AS PARENT, FLOOR.ACTIVE_IND,
                BUILDING.NAME AS BUILDING_NAME, FLOOR.NAME AS FLOOR_NAME,
                SITE.SITE_ID
           FROM LOCN_LOCATION LOC, LOCN_SITE SITE, LOCN_BUILDING BUILDING,
                LOCN_FLOOR FLOOR, LOCN_LOCATION LOC1
          WHERE     LOC.SITE_ID = SITE.SITE_ID
                AND LOC.BUILDING_ID = BUILDING.BUILDING_ID
                AND LOC.BUILDING_ID = FLOOR.BUILDING_ID
                AND LOC.FLOOR_ID = FLOOR.FLOOR_ID
                AND LOC.SITE_ID || LOC.BUILDING_ID =
                          LOC1.SITE_ID
                       || LOC1.BUILDING_ID
                       || LOC1.FLOOR_ID
                       || LOC1.ROOM_ID
                AND LOC.ROOM_ID IS NULL;

      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_FLOOR_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_LOCATION';
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR LOCATION_REC IN LOCATION_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         LOCATION_REC.LOCATION_ID := LOCATION_REC.LOCATION_ID;

         VAR_DESCRIPTION :=
               LOCATION_REC.SITE_ID
            || ' '
            || LOCATION_REC.BUILDING_NAME
            || ' '
            || LOCATION_REC.FLOOR_NAME;

         VAR_STATUS := NULL;

         -- Check if the qv location existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_LOCATION
             WHERE LOCATION_ID = LOCATION_REC.LOCATION_ID;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Check if the record existed and same in Trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_LOCATION
                WHERE     LOCATION_ID = LOCATION_REC.LOCATION_ID
                      AND NVL(PARENT, '0') = NVL(LOCATION_REC.PARENT, '0')
                      AND DESCRIPTION = VAR_DESCRIPTION
                      AND ACTIVE_IND = LOCATION_REC.ACTIVE_IND;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_STATUS := 'MODIFIED';
            END;
         END IF;

         IF VAR_STATUS = 'MODIFIED'
         THEN
            VAR_MODIFIED_DT := SYSDATE;
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, SYSDATE, '3', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         ELSE
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, VAR_CREATED_DT, '3', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT LOCATION_ID
                 FROM TRIM_LOCATION
                WHERE DEPTH = '3' AND CANCEL_DT IS NULL
               MINUS
               SELECT LOCN_LOCATION.LOCATION_ID
                 FROM LOCN_LOCATION, LOCN_SITE, LOCN_LOCATION LOCN_B,
                      LOCN_BUILDING, LOCN_FLOOR
                WHERE LOCN_LOCATION.SITE_ID = LOCN_SITE.SITE_ID
                      AND LOCN_LOCATION.BUILDING_ID =
                             LOCN_BUILDING.BUILDING_ID
                      AND LOCN_LOCATION.BUILDING_ID = LOCN_FLOOR.BUILDING_ID
                      AND LOCN_LOCATION.FLOOR_ID = LOCN_FLOOR.FLOOR_ID
                      AND LOCN_LOCATION.SITE_ID || LOCN_LOCATION.BUILDING_ID =
                                LOCN_B.SITE_ID
                             || LOCN_B.BUILDING_ID
                             || LOCN_B.FLOOR_ID
                             || LOCN_B.ROOM_ID
                      AND LOCN_LOCATION.ROOM_ID IS NULL;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_LOCATION T
                     SET T.CANCEL_DT = SYSDATE
                   WHERE T.LOCATION_ID = ROW.LOCATION_ID;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.LOCATION_ID || '|' || SYSDATE);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_FLOOR_IU;


   PROCEDURE TRIM_ROOM_IU
   IS
      ------------------------------------------
      -- Constellar Hub : For each Record in Collate Cursor
      ------------------------------------------
      CURSOR LOCATION_CURSOR
      IS
         SELECT LOC.LOCATION_ID, LOC1.LOCATION_ID AS PARENT, ROOM.ACTIVE_IND,
                BUILDING.NAME AS BUILDING_NAME, FLOOR.NAME AS FLOOR_NAME,
                ROOM.ROOM_ID, SITE.SITE_ID
           FROM LOCN_LOCATION LOC, LOCN_SITE SITE, LOCN_BUILDING BUILDING,
                LOCN_FLOOR FLOOR, LOCN_ROOM ROOM, LOCN_LOCATION LOC1
          WHERE     LOC.SITE_ID = SITE.SITE_ID
                AND LOC.BUILDING_ID = BUILDING.BUILDING_ID
                AND LOC.BUILDING_ID = FLOOR.BUILDING_ID
                AND LOC.FLOOR_ID = FLOOR.FLOOR_ID
                AND LOC.BUILDING_ID = ROOM.BUILDING_ID
                AND LOC.FLOOR_ID = ROOM.FLOOR_ID
                AND LOC.ROOM_ID = ROOM.ROOM_ID
                AND LOC.SITE_ID || LOC.BUILDING_ID || LOC.FLOOR_ID =
                          LOC1.SITE_ID
                       || LOC1.BUILDING_ID
                       || LOC1.FLOOR_ID
                       || LOC1.ROOM_ID
                AND LOC.ROOM_ID IS NOT NULL;

      VAR_TRANSACTION_NAME   VARCHAR2(4000) := 'TRIM_ROOM_IU';
      VAR_TABLE_NAME         VARCHAR2(4000) := 'TRIM_LOCATION';
      VAR_DESCRIPTION        VARCHAR2(4000);
      VAR_STATUS             VARCHAR2(4000);
      VAR_CREATED_DT         DATE;
      VAR_MODIFIED_DT        DATE;
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Pre Record Actions
      ------------------------------------------
      FOR LOCATION_REC IN LOCATION_CURSOR
      LOOP
         ------------------------------------------
         -- Constellar Hub : (Optional) Replicate "Each Record Actions" block below
         ------------------------------------------
         ------------------------------------------
         -- Constellar Hub : Each Record Actions
         ------------------------------------------
         -- Prepare value for each field in the records and set them to a local variable
         LOCATION_REC.LOCATION_ID := LOCATION_REC.LOCATION_ID;
         VAR_DESCRIPTION :=
               LOCATION_REC.SITE_ID
            || ' '
            || LOCATION_REC.BUILDING_NAME
            || ' '
            || LOCATION_REC.FLOOR_NAME
            || ' '
            || LOCATION_REC.ROOM_ID;
         LOCATION_REC.ACTIVE_IND := LOCATION_REC.ACTIVE_IND;
         VAR_STATUS := NULL;

         -- Check if the qv location existed in trim
         BEGIN
            SELECT CREATED_DT, MODIFIED_DT
              INTO VAR_CREATED_DT, VAR_MODIFIED_DT
              FROM TRIM_LOCATION
             WHERE LOCATION_ID = LOCATION_REC.LOCATION_ID;
         EXCEPTION
            WHEN NO_DATA_FOUND
            THEN
               VAR_STATUS := 'CREATE';
         END;

         -- Check if the record existed and same in Trim
         IF VAR_STATUS IS NULL
         THEN
            BEGIN
               SELECT MODIFIED_DT
                 INTO VAR_MODIFIED_DT
                 FROM TRIM_LOCATION
                WHERE     LOCATION_ID = LOCATION_REC.LOCATION_ID
                      AND NVL(PARENT, '0') = NVL(LOCATION_REC.PARENT, '0')
                      AND DESCRIPTION = VAR_DESCRIPTION
                      AND ACTIVE_IND = LOCATION_REC.ACTIVE_IND;
            EXCEPTION
               WHEN NO_DATA_FOUND
               THEN
                  VAR_STATUS := 'MODIFIED';
            END;
         END IF;

         IF VAR_STATUS = 'MODIFIED'
         THEN
            VAR_MODIFIED_DT := SYSDATE;
         END IF;

         ------------------------------------------
         -- Constellar Hub : Actions = Insert, Update or Delete
         ------------------------------------------
         -- Perform insert, upsert or delete functions
         IF VAR_STATUS = 'CREATE'
         THEN
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, SYSDATE, '4', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         ELSE
            SET_TRIM_LOCATION_REC(VAR_STATUS, VAR_TRANSACTION_NAME, LOCATION_REC.LOCATION_ID, LOCATION_REC.ACTIVE_IND, NULL, VAR_CREATED_DT, '4', VAR_DESCRIPTION, VAR_MODIFIED_DT, LOCATION_REC.PARENT);
         END IF;
      ------------------------------------------
      -- Constellar Hub : Post Record Actions
      ------------------------------------------
      END LOOP;

      ------------------------------------------
      -- Constellar Hub : Post Transaction Actions
      ------------------------------------------
      BEGIN
         DECLARE
            CURSOR POST_CURSOR
            IS
               SELECT LOCATION_ID
                 FROM TRIM_LOCATION
                WHERE DEPTH = '4' AND CANCEL_DT IS NULL
               MINUS
               SELECT LOCN_LOCATION.LOCATION_ID
                 FROM LOCN_LOCATION, LOCN_SITE, LOCN_LOCATION LOCN_B,
                      LOCN_BUILDING, LOCN_FLOOR, LOCN_ROOM
                WHERE LOCN_LOCATION.SITE_ID = LOCN_SITE.SITE_ID
                      AND LOCN_LOCATION.BUILDING_ID =
                             LOCN_BUILDING.BUILDING_ID
                      AND LOCN_LOCATION.BUILDING_ID = LOCN_FLOOR.BUILDING_ID
                      AND LOCN_LOCATION.FLOOR_ID = LOCN_FLOOR.FLOOR_ID
                      AND LOCN_LOCATION.BUILDING_ID = LOCN_ROOM.BUILDING_ID
                      AND LOCN_LOCATION.FLOOR_ID = LOCN_ROOM.FLOOR_ID
                      AND LOCN_LOCATION.ROOM_ID = LOCN_ROOM.ROOM_ID
                      AND    LOCN_LOCATION.SITE_ID
                          || LOCN_LOCATION.BUILDING_ID
                          || LOCN_LOCATION.FLOOR_ID =
                                LOCN_B.SITE_ID
                             || LOCN_B.BUILDING_ID
                             || LOCN_B.FLOOR_ID
                             || LOCN_B.ROOM_ID
                      AND LOCN_LOCATION.ROOM_ID IS NOT NULL;
         BEGIN
            FOR ROW IN POST_CURSOR
            LOOP
               BEGIN
                  UPDATE TRIM_LOCATION T
                     SET T.CANCEL_DT = SYSDATE
                   WHERE T.LOCATION_ID = ROW.LOCATION_ID;

                  HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'DEBUG', VAR_TABLE_NAME, 'POST TRANSACTION UPDATE', ROW.LOCATION_ID || '|' || SYSDATE);
               END;
            END LOOP;

            COMMIT;
         EXCEPTION
            -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
            WHEN OTHERS
            THEN
               HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, VAR_TRANSACTION_NAME, 'ERROR', VAR_TABLE_NAME, 'POST TRANSACTION WHEN OTHERS EXCEPTION', SQLERRM);
         END;
      END;
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, 'TRIM_ROOM_IU', 'ERROR', VAR_TABLE_NAME, 'WHEN OTHERS EXCEPTION', SQLERRM);
   END TRIM_ROOM_IU;

   PROCEDURE POST_INTERFACE
   IS
   BEGIN
      ------------------------------------------
      -- Constellar Hub : Post Interface Rule
      ------------------------------------------
      HUB_LIB.SET_RUN_DATES(GC_INTERFACE_NAME, G_RUN_START, G_RUN_END);
   EXCEPTION
      -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
      WHEN OTHERS
      THEN
         HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, 'POST_INTERFACE', 'ERROR', NULL, 'POST_INTERFACE WHEN OTHERS EXCEPTION', SQLERRM);
   END POST_INTERFACE;

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
         L_PHASE := 'EXECUTE PRE_INTERFACE';
         --PRE_INTERFACE;

         L_PHASE := 'EXECUTE TRANSACTIONS';
         TRIM_SITE_IU;
         TRIM_BUILDING_IU;
         TRIM_FLOOR_IU;
         TRIM_ROOM_IU;

         L_PHASE := 'EXECUTE POST_INTERFACE';
      --POST_INTERFACE;
      EXCEPTION
         -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.
         WHEN OTHERS
         THEN
            HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'ERROR', NULL, 'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: ' || L_PHASE, SQLERRM);
      END;

      -- Log that this interface has finished.
      L_END_TIME := LOCALTIMESTAMP;
      L_ELAPSED_TIME := L_END_TIME - L_START_TIME;
      HUB_LOG.LOG_WRITE(GC_INTERFACE_NAME, C_THIS_PROC, 'INFO', NULL, 'Elapsed time ' || L_ELAPSED_TIME, 'Ended at: ' || TO_CHAR(L_END_TIME));
   END MAIN_CONTROL;
END TRIM_FROM_QV;
/
