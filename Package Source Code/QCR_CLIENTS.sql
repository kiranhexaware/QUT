CREATE OR REPLACE PACKAGE QCR_CLIENTS AS

  PROCEDURE Main_Control;
                               
END QCR_CLIENTS;
/


CREATE OR REPLACE PACKAGE BODY               QCR_CLIENTS AS
   gc_interface_name    CONSTANT VARCHAR2 (20) := 'QCR_CLIENTS';
      
   PROCEDURE Process_Clients
               (p_role_cd  IN CDS_CLIENT_ROLE.role_cd%TYPE,
                p_start_dt IN DATE)  IS
       
      c_trans_name CONSTANT VARCHAR2(60) := 'PROCESS_CLIENTS';
      
      -- "Dawn of Time" date used for optional parts of cursor
      c_low_dt     DATE := TO_DATE('01-JAN-1900','DD-MON-YYYY');
      
      l_phase      VARCHAR2(60)          := 'Initialising';

      l_err        VARCHAR2(1000)        := NULL;
      l_rec_count  NUMBER                := 0;
      l_err_count  NUMBER                := 0;
      l_wrn_count  NUMBER                := 0;
      
      ------------------------------------------
      -- QCR Clients cursor
      ------------------------------------------                    
      CURSOR qcr_clients_cr
              (p_role_cd CDS_CLIENT_ROLE.role_cd%TYPE,
               p_from_dt DATE) IS
       (SELECT * FROM 
         (SELECT ccr.client_id
                ,ccr.client_role_id
                ,ccr.role_cd
                ,ccr.role_identity
                ,ccr.trs_client_id
                ,ccr.access_name
                ,ccr.email_alias
                ,ccr.email_domain
                ,DECODE(
                    DECODE(ccr.role_cd, 
                           'STAFF',   cca_sf.attribute_value,
                           'STUDENT', cca_st.attribute_value,
                                      cca_ot.attribute_value
                          ),'TRUE', 'Y',
                                    'N')     AS "ACTIVE_ROLE"
                ,cca_tl.attribute_value      AS "TITLE"
                ,cca_sn.attribute_value      AS "SURNAME"
                ,cca_fn.attribute_value      AS "FIRST_NAME"
                ,cca_pn.attribute_value      AS "PREFERRED_NAME"
                ,cca_on.attribute_value      AS "OTHER_NAMES"
                ,ip.primary_extn             AS "PHONE"
                ,ip.primary_fax              AS "FAX"
                ,ip.mobile                   AS "MOBILE"
                ,GREATEST(NVL(ccr.updated_dt,   ccr.inserted_dt),    
                          -- COALESCE neccesary here because OUTER JOIN on 
                          -- these attributes can return NULL for both dates
                          COALESCE(cca_sf.updated_dt, cca_sf.inserted_dt, c_low_dt),
                          COALESCE(cca_st.updated_dt, cca_st.inserted_dt, c_low_dt),
                          COALESCE(cca_ot.updated_dt, cca_ot.inserted_dt, c_low_dt),
                          NVL(cca_pd.updated_dt, cca_pd.inserted_dt), 
                          NVL(cca_tl.updated_dt, cca_tl.inserted_dt), 
                          NVL(cca_sn.updated_dt, cca_sn.inserted_dt), 
                          NVL(cca_fn.updated_dt, cca_fn.inserted_dt),
                          -- COALESCE neccesary here because OUTER JOIN on 
                          -- these attributes can return NULL for both dates
                          COALESCE(cca_pn.updated_dt, cca_pn.inserted_dt, c_low_dt),
                          COALESCE(cca_on.updated_dt, cca_on.inserted_dt, c_low_dt), 
                          NVL(ip.update_on, c_low_dt)
                         )                   AS "UPDATED_DT"
            FROM cds_client_role       ccr
                ,cds_client_attribute  cca_sf
                ,cds_client_attribute  cca_st
                ,cds_client_attribute  cca_ot
                ,cds_client_attribute  cca_pd
                ,cds_client_attribute  cca_tl
                ,cds_client_attribute  cca_sn
                ,cds_client_attribute  cca_fn
                ,cds_client_attribute  cca_pn
                ,cds_client_attribute  cca_on
                ,ip                    ip
           WHERE ccr.client_id          = cca_sf.client_id(+)
             AND ccr.client_role_id     = cca_sf.parent_client_attribute_id(+)
             AND cca_sf.attribute_cd(+) = 'CURRENT_STAFF'
             AND ccr.client_id          = cca_st.client_id(+)
             AND ccr.client_role_id     = cca_st.parent_client_attribute_id(+)
             AND cca_st.attribute_cd(+) = 'CURRENT_STUDENT'
             AND ccr.client_id          = cca_ot.client_id(+)
             AND ccr.client_role_id     = cca_ot.parent_client_attribute_id(+)
             AND cca_ot.attribute_cd(+) = 'CURRENT_OTHER'
             AND ccr.client_id       = cca_pd.client_id
             AND ccr.client_role_id  = cca_pd.parent_client_attribute_id
             AND cca_pd.attribute_cd = 'PERSONAL_DETAILS'
             AND cca_pd.client_id            = cca_tl.client_id(+)
             AND cca_pd.client_attribute_id  = cca_tl.parent_client_attribute_id(+)
             AND cca_tl.attribute_cd(+)      = 'TITLE'
             AND cca_pd.client_id            = cca_sn.client_id
             AND cca_pd.client_attribute_id  = cca_sn.parent_client_attribute_id
             AND cca_sn.attribute_cd         = 'SURNAME'
             AND cca_pd.client_id            = cca_fn.client_id(+)
             AND cca_pd.client_attribute_id  = cca_fn.parent_client_attribute_id(+)
             AND cca_fn.attribute_cd(+)      = 'FIRST_NAME'
             AND cca_pd.client_id            = cca_pn.client_id(+)
             AND cca_pd.client_attribute_id  = cca_pn.parent_client_attribute_id(+)
             AND cca_pn.attribute_cd(+)      = 'PREFERRED_NAME'
             AND cca_pd.client_id            = cca_on.client_id(+)
             AND cca_pd.client_attribute_id  = cca_on.parent_client_attribute_id(+)
             AND cca_on.attribute_cd(+)      = 'OTHER_NAMES'
             AND ip.ip_num(+) = ccr.trs_client_id
         )
        WHERE role_cd = p_role_cd
          AND updated_dt >= p_from_dt
       );

   BEGIN
      l_phase := 'Starting';
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'TRANSACTION started for role: '||p_role_cd);
            
      <<clientloop>>
      FOR client_rec IN qcr_clients_cr(p_role_cd, p_start_dt)
      LOOP
        BEGIN
          IF  client_rec.email_alias IS NOT NULL
          THEN
            l_phase := 'Set record';
            QCR_SOA_CLIENT.set_ClientRole
              (p_SourceFeedName     => 'NOVO'
              ,p_SourceFeedVer      => '0.3'
              ,p_IAM_ClientID       => client_rec.client_id
              ,p_IAM_RoleCd         => client_rec.role_cd
              ,p_IAM_RoleID         => client_rec.role_identity
              ,p_EmailAddress       => RTRIM(client_rec.email_alias||'@'||
                                             client_rec.email_domain, 
                                             '@')
              ,p_AccessUsername     => client_rec.access_name
              ,p_Title              => client_rec.title
              ,p_GivenName          => client_rec.first_name
              ,p_PreferredName      => client_rec.preferred_name
              ,p_Surname            => client_rec.surname
              ,p_IAM_RoleActiveInd  => client_rec.active_role
              ,p_ErrorMsg           => l_err
              ,p_Phone              => client_rec.phone
              ,p_Fax                => client_rec.fax
              ,p_Mobile             => client_rec.mobile);

             l_rec_count := l_rec_count + 1;
             IF l_err IS NOT NULL THEN
               l_err_count := l_err_count + 1;
               -- record the error
               HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL 
                                  , 'access_name:'||client_rec.access_name||
                                    ' role_cd:'||client_rec.role_cd||
                                    ' ident:'||client_rec.role_identity
                                  , 'QCR_SOA_CLIENT.set_ClientRole returned: '||l_err);
             END IF; -- check l_err
          ELSE
            -- Report NULL data as WARN
            l_wrn_count := l_wrn_count + 1;
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'WARN', NULL 
                              , ' role_cd:'||client_rec.role_cd||
                                ' ident:'||client_rec.role_identity
                              , 'Access name or email details are NULL - record skipped');
          END IF; --Check for NULLs
        EXCEPTION
          WHEN OTHERS THEN
            HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL 
                              , 'access_name:'||client_rec.access_name||
                                ' role_cd:'||client_rec.role_cd||
                                ' ident:'||client_rec.role_identity
                              , 'Transaction failure during Phase: '||l_phase||
                                ' - '||SQLERRM);
        END;
      END LOOP clientloop;
    
      -- report completion status
      HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'INFO', NULL, NULL, 
                         'TRANSACTION completed for role: '||
                         p_role_cd||'- processed '||
                         l_rec_count||' records with '||
                         l_err_count||' errors and '||
                         l_wrn_count||' warnings.');
                                              
   EXCEPTION
      WHEN OTHERS THEN
         HUB_LOG.LOG_WRITE (gc_interface_name, c_trans_name, 'ERROR', NULL, NULL, SQLERRM);
         RAISE;                       
   END Process_Clients;
   

   PROCEDURE MAIN_CONTROL IS
     c_this_proc      CONSTANT VARCHAR2(20) := 'MAIN_CONTROL';
     l_phase          VARCHAR2(50)          := 'Initialising';
     l_start_ts       TIMESTAMP;
     l_run_start_ts   TIMESTAMP := SYSTIMESTAMP;
     l_start_dt       DATE;
     l_end_ts         TIMESTAMP;
     l_elapsed_time   INTERVAL DAY (5) TO SECOND (6);    
   BEGIN
      HUB_LIB.GET_RUN_DATES(gc_INTERFACE_NAME, l_start_ts, l_end_ts);

      -- Log that this interface has started.
      HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                        'Starting ' || gc_interface_name, 
                        'Start at: ' || TO_CHAR (l_start_ts));
            
      BEGIN
        l_start_dt := TO_DATE(TO_CHAR(l_start_ts, 'DD-MON-YYYY'), 'DD-MON-YYYY');
        
        l_phase := 'Process STAFF Clients';
        Process_Clients('STAFF', l_start_dt);
         
        l_phase := 'Process STUDENT Clients';
        Process_Clients('STUDENT', l_start_dt);
         
        l_phase := 'Process VISITOR Clients';
        Process_Clients('VISITOR', l_start_dt);
         
        -- Update last run date if no errors
        HUB_LIB.SET_RUN_DATES(gc_INTERFACE_NAME, l_start_ts, l_end_ts);
                      
      EXCEPTION
        -- Catch any exceptions in order to end gracefully and not affect the logging of start and end message.        
        WHEN OTHERS THEN
           HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'ERROR', NULL, 
           'MAIN_CONTROL WHEN OTHERS EXCEPTION during Phase: '||l_phase,
           SQLERRM);
      END;

      -- Log that this interface has finished.
      l_end_ts := SYSTIMESTAMP;
      l_elapsed_time := l_end_ts - l_run_start_ts;
      HUB_LOG.LOG_WRITE (gc_interface_name, c_this_proc, 'INFO', NULL, 
                        'Elapsed time ' || l_elapsed_time, 
                        'Ended at: ' || TO_CHAR (l_end_ts));
   END MAIN_CONTROL;
  
END QCR_CLIENTS;
/
