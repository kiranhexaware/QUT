CREATE OR REPLACE PACKAGE HUB_LOG
AS
   /**
    * A procedure that logs in to a common logging table for OMG HUB system. Only does inserts.
    * p_interface_name: The interface package name that is doing this logging.
    * p_process: The internal procedure or function or sub block within this interface package that is doing this logging.
    * p_key_data: The key data that are affected by this log message in the format of "name: value | name2: value2" format e.g. "unit_cd: AYN453 | unit_version: 5 | "
    * p_message: The descriptive messsage to log.
    * p_event_type: The type of log message, usually "INFO" "WARN" "ERROR"
    * p_table: [optional field] any underlying tables that the execution thread is currently working on when this log happens
    */
    PROCEDURE LOG_WRITE (p_interface_name  IN VARCHAR2 := 'UNKNOWN INTERFACE',
                        p_process          IN VARCHAR2 := 'UNKNOWN',
                        p_event_type       IN VARCHAR2 := 'INFO',
                        p_tables           IN VARCHAR2 := NULL,
                        p_key_data         IN VARCHAR2,
                        p_message          IN VARCHAR2
                        );

   /**
    * A procedure that reads the current logging level, i.e. "DEBUG" or "INFO" for the entire environment
    */
   PROCEDURE READ_LOG_LEVEL;

   /**
    * A procedure that sets the logging level, i.e. "DEBUG" or "INFO" for the entire environment
    */
   PROCEDURE SET_LOG_LEVEL(p_log_level VARCHAR2);

   PROCEDURE LOG_START ( p_interface_name   IN VARCHAR2 := 'UNKNOWN INTERFACE');

   PROCEDURE LOG_STOP ( p_interface_name   IN VARCHAR2 := 'UNKNOWN INTERFACE');

   -- Read the current log archive values from the database
   PROCEDURE READ_ARCHIVE_VALUES;

   -- Set the log archive control values
   PROCEDURE SET_ARCHIVE_VALUES ( p_days_before_archive IN NUMBER,
                                  p_days_before_delete IN NUMBER);

   /*
      Move old log records from HUB_EVENT_LOG table to _ARCHIVE table
      Delete very old log records from _ARCHIVE table

      Log age definitions controlled by system values set via SET_ARCHIVE_VALUES
      these system values are required - ARCHIVE_LOG will do nothing if they are not set.
   */
   PROCEDURE ARCHIVE_LOG;

END HUB_LOG;
/


CREATE OR REPLACE PACKAGE BODY HUB_LOG

-- Deployed on: Mon Apr  7 15:18:39 EST 2014
-- Deployed from: intdeploy.qut.edu.au:/home/integsvc/novoP/hub/HUB_LOG/tags/1.2.0/Packages/HUB_LOG.pkb

/*
Package of to do standard logging for the OMG_HUB system.
In order for DEBUG to work, a hub value of 'LOG_LEVEL' needs to be set to 'DEBUG'
by calling SET_LOG_LEVEL procedure.

04 Feb 2011  WK Tio      Added Debug function
22 Mar 2011  D Peterson  Added LOG_START and LOG_STOP procedures
*/

AS
   gc_interface_name CONSTANT VARCHAR2 (8) := 'HUB_LOG';
   g_log_level                VARCHAR2 (20);
   g_days_before_archive      NUMBER;
   g_days_before_delete       NUMBER;

   PROCEDURE LOG_WRITE (p_interface_name   IN VARCHAR2 := 'UNKNOWN INTERFACE',
                        p_process          IN VARCHAR2 := 'UNKNOWN',
                        p_event_type       IN VARCHAR2 := 'INFO', -- Event type should be either 'INFO', 'WARN', 'ERROR', 'DEBUG'
                        p_tables           IN VARCHAR2 := NULL,
                        p_key_data         IN VARCHAR2,
                        p_message          IN VARCHAR2
                        )
   IS
      PRAGMA AUTONOMOUS_TRANSACTION;
   BEGIN
      -- Check the log level
      IF p_event_type = 'DEBUG' AND g_log_level <> 'DEBUG' THEN
        RETURN;
      ELSE

          INSERT INTO HUB_EVENT_LOG
               VALUES (p_interface_name,
                       p_process,
                       SYSTIMESTAMP,
                       p_event_type,
                       p_tables,
                       p_key_data,
                       p_message);

          COMMIT;
      END IF;
   END LOG_WRITE;


   PROCEDURE READ_LOG_LEVEL
   -- Read the logging level set in the database.
   IS

   BEGIN
        HUB_LIB.GET_HUB_VALUE (gc_interface_name, 'LOG_LEVEL', g_log_level);
   END READ_LOG_LEVEL;

   PROCEDURE SET_LOG_LEVEL(p_log_level VARCHAR2)
   --
   IS

   BEGIN
        HUB_LIB.SET_HUB_VALUE (gc_interface_name, 'LOG_LEVEL', p_log_level);
        READ_LOG_LEVEL;
   END SET_LOG_LEVEL;


  PROCEDURE LOG_START ( p_interface_name   IN VARCHAR2 := 'UNKNOWN INTERFACE'
                      ) IS
   BEGIN
     LOG_WRITE (p_interface_name,
                ' ',
                'START',
                NULL,
                NULL,
                'Starting interface '||p_interface_name||
                ' at '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   END LOG_START;


  PROCEDURE LOG_STOP ( p_interface_name   IN VARCHAR2 := 'UNKNOWN INTERFACE'
                     ) IS
   l_Start_Time    TIMESTAMP;
   l_End_Time      TIMESTAMP;
   l_Elapsed_Time  INTERVAL DAY(2) TO SECOND(6);
   l_Key_Data       VARCHAR(4000);

   BEGIN
    SELECT max(proc_timestamp)
         INTO l_Start_Time
         FROM hub_event_log
         WHERE interface_name = p_interface_name
         AND event_type = 'START';

     l_End_Time := LOCALTIMESTAMP;
     l_Elapsed_Time := l_End_Time - l_Start_Time;
     l_Key_Data := 'Elapsed time ' ||  l_Elapsed_Time;

     LOG_WRITE (p_interface_name,
                ' ',
                'STOP',
                NULL,
                l_Key_Data,
                'Stopping interface '||p_interface_name||
                ' at '||TO_CHAR(SYSDATE, 'DD-MON-YYYY HH24:MI:SS'));
   END LOG_STOP;


   PROCEDURE READ_ARCHIVE_VALUES IS
      -- Read current archive settings in database
   BEGIN
      HUB_LIB.GET_HUB_VALUE (gc_interface_name, 'DAYS_BEFORE_ARCHIVE', g_days_before_archive);
      HUB_LIB.GET_HUB_VALUE (gc_interface_name, 'DAYS_BEFORE_DELETE', g_days_before_delete);
   END;

   /*

   */
   PROCEDURE SET_ARCHIVE_VALUES ( p_days_before_archive NUMBER,
                                  p_days_before_delete NUMBER) IS
      -- Convenience procedure to control archive settings
   BEGIN
      HUB_LIB.SET_HUB_VALUE (gc_interface_name, 'DAYS_BEFORE_ARCHIVE', p_days_before_archive);
      HUB_LIB.SET_HUB_VALUE (gc_interface_name, 'DAYS_BEFORE_DELETE', p_days_before_delete);
      READ_ARCHIVE_VALUES;
   END;

   /*
      Move old log records from HUB_EVENT_LOG table to _ARCHIVE table
      Delete very old log records from _ARCHIVE table
      Log age definitions controlled by system values set via SET_ARCHIVE_VALUES
   */
   PROCEDURE ARCHIVE_LOG IS
      c_proc_name       CONSTANT VARCHAR2(60) := 'ARCHIVE_LOG';
      l_start_time      TIMESTAMP;
   BEGIN
      l_start_time := sysdate;

      LOG_WRITE (gc_interface_name,
                  c_proc_name,
                  'INFO',
                  NULL,
                  'Archive/Delete days: '||g_days_before_archive||'/'||g_days_before_delete||' | Start time: '||l_start_time,
                  'Beginning HUB_LOG archive process.');

      -- Move old logs to _Archive table
      IF g_days_before_archive IS NOT NULL THEN
         BEGIN
            INSERT INTO hub_event_log_archive
               (SELECT *
               FROM hub_event_log
               WHERE proc_timestamp < l_start_time - g_days_before_archive);

            DELETE FROM hub_event_log
            WHERE proc_timestamp < l_start_time - g_days_before_archive;

            COMMIT;
         EXCEPTION
            WHEN OTHERS THEN
               LOG_WRITE (gc_interface_name,
                           c_proc_name,
                           'ERROR',
                           NULL,
                           NULL,
                           'Error while moving logs to Archive table : '||SQLERRM);
               ROLLBACK;
         END;
      END IF;

      -- Delete very old logs from _Archive table
      IF g_days_before_delete IS NOT NULL THEN
         BEGIN

            DELETE FROM hub_event_log_archive
            WHERE proc_timestamp < l_start_time - g_days_before_delete;

            COMMIT;
         EXCEPTION
            WHEN OTHERS THEN
               LOG_WRITE (gc_interface_name,
                           c_proc_name,
                           'ERROR',
                           NULL,
                           NULL,
                           'Error while deleting logs from Archive table : '||SQLERRM);
               ROLLBACK;
         END;
      END IF;

      LOG_WRITE (gc_interface_name,
                  c_proc_name,
                  'INFO',
                  NULL,
                  NULL,
                  'HUB_LOG Archive complete');
   END ARCHIVE_LOG;

BEGIN
    -- Initialise this package by reading the log level from the database
    READ_LOG_LEVEL;
    -- And the archive settings
    READ_ARCHIVE_VALUES;
END HUB_LOG;
/
