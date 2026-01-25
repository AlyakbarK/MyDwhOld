create or replace package ETL_MAIN is

  -- Created : 17.02.2017 10:08:24
  -- Purpose :

  type REC_TYPE is ref cursor;

  function GET_C_COMMIT_COUNT return Number;

  function GET_C_BEGIN_DATE return Date;

  function GET_C_END_DATE return Date;

  function GET_C_NULL_ID return Number;

  function GET_C_MAX_ID return Number;

  /* Процедура вызывается перед вставкой записей для отключения локальных индексов заданной секции */
  procedure beforeLoad(aPartName In Varchar2, aTableName In Varchar2);

  /* Процедура вызывается после вставки записей для перестроения локальных индексов заданной секции */
  procedure afterLoad(aPartName In Varchar2, aTableName In Varchar2);

  /* Запись логов */
  procedure pWriteMatViewLog(nSTART_ID   in number,
                             aMViewName  in varchar2,
                             aLogMsg     in varchar2,
                             tstart_time in date,
                             tstop_time  in date,
                             fis_error   in number);

  /* Запись логов DIM */
  procedure pInsertDimLog(vDIM_ID in number, vWORK_DATE in date);

  /* Обновление логов DIM */
  procedure pUpdateDimLog(vDIM_ID    in number,
                          vWORK_DATE in date,
                          vLOG_MSG   in varchar2,
                          vEND_TIME  in date,
                          vIS_ERROR  in number);

  /* Запись логов FACT */
  procedure pInsertFactLog(vFACT_ID in number, vWORK_DATE in date);

  /* Обновление логов FACT */
  procedure pUpdateFactLog(vFACT_ID   in number,
                           vWORK_DATE in date,
                           vLOG_MSG   in varchar2,
                           vEND_TIME  in date,
                           vIS_ERROR  in number);

  procedure refresh_mview(nstart_id in number default -1, th in number);
  procedure refresh_mtable(nstart_id in number default -1,
                           wd        in date,
                           mtable    in varchar2);
  procedure refresh_dim(wd date, thread number);
  procedure refresh_custom(wd date, thread number);
  procedure rollback_dim(vt in varchar2, wd date default p_operday);
  procedure monitoring_dim(vwd date default p_operday);
  procedure refresh_fact(wd date, thread number);

  /* Сбор статистики */
  procedure gather_stat(aTableName In Varchar2 default null,
                        aPartName  In Varchar2 default null,
                        aDate      In Date);

  procedure start_etl;

end ETL_MAIN;
/
create or replace package body ETL_MAIN is

  C_COMMIT_COUNT constant Number(4) := 5000;
  C_BEGIN_DATE   constant Date := to_date('01.01.1001', 'dd.mm.yyyy');
  C_END_DATE     constant Date := to_date('31.12.9999', 'dd.mm.yyyy');
  C_NULL_ID      constant Number(3) := -999;
  C_MAX_ID       constant Number(18) := 999999999999999999;

  function GET_C_COMMIT_COUNT return Number is
  begin
    return C_COMMIT_COUNT;
  end;

  function GET_C_BEGIN_DATE return Date is
  begin
    return C_BEGIN_DATE;
  end;

  function GET_C_END_DATE return Date is
  begin
    return C_END_DATE;
  end;

  function GET_C_NULL_ID return Number is
  begin
    return C_NULL_ID;
  end;

  function GET_C_MAX_ID return Number is
  begin
    return C_MAX_ID;
  end;

  /* Процедура вызывается перед вставкой записей для отключения локальных индексов заданной секции */
  procedure beforeLoad(aPartName In Varchar2, aTableName In Varchar2) is
    vPartName  Varchar2(30);
    vTableName Varchar2(35);
  begin
    vPartName  := upper(aPartName);
    vTableName := upper(aTableName);
  
    execute immediate ('alter table ' || vTableName || ' modify partition ' ||
                      vPartName || ' unusable local indexes');
    execute immediate ('alter session set skip_unusable_indexes = true');
  end;

  /* Процедура вызывается после вставки записей для перестроения локальных индексов заданной секции */
  procedure afterLoad(aPartName In Varchar2, aTableName In Varchar2) is
    vPartName  Varchar2(30);
    vTableName Varchar2(35);
  begin
    vPartName  := upper(aPartName);
    vTableName := upper(aTableName);
  
    for idx in (select ui.index_name
                  from user_indexes ui
                 where ui.table_name = vTableName) loop
      execute immediate ('alter index ' || idx.index_name ||
                        ' rebuild partition ' || vPartName || ' nologging');
    end loop;
  end;

  /* Запись логов Mat View*/
  procedure pWriteMatViewLog(nSTART_ID   in number,
                             aMViewName  in varchar2,
                             aLogMsg     in varchar2,
                             tstart_time in date,
                             tstop_time  in date,
                             fis_error   number) is
    --pragma autonomous_transaction;
  begin
    insert into mview_log
      select sysdate,
             aMViewName,
             aLogMsg,
             tstart_time,
             tstop_time,
             fis_error,
             round((tstop_time - tstart_time) * 24, 2),
             round((tstop_time - tstart_time) * 24 * 60, 2),
             nSTART_ID
        from dual;
    commit;
  end;

  /* Запись логов DIM */
  procedure pInsertDimLog(vDIM_ID in number, vWORK_DATE in date) is
  begin
  
    insert into SYSTEM_REFRESH_DIM_LOG t
      select vDIM_ID    dim_id,
             vWORK_DATE work_date,
             sysdate    log_date,
             null       log_msg,
             sysdate    start_time,
             null       end_time,
             null       is_error,
             null       in_hour,
             null       in_min
        from dual;
    commit;
  exception
    when others then
      null;
  end;

  /* Обновление логов DIM */
  procedure pUpdateDimLog(vDIM_ID    in number,
                          vWORK_DATE in date,
                          vLOG_MSG   in varchar2,
                          vEND_TIME  in date,
                          vIS_ERROR  in number) is
  begin
  
    update SYSTEM_REFRESH_DIM t
       set t.last_refresh_date_wd = vWORK_DATE,
           t.is_error             = vIS_ERROR,
           t.th_end_time          = sysdate
     where t.id = vDIM_ID;
  
    update SYSTEM_REFRESH_DIM_LOG t
       set t.log_date = sysdate,
           t.log_msg  = vLOG_MSG,
           t.end_time = vEND_TIME,
           t.is_error = vIS_ERROR,
           t.in_hour  = round((vEND_TIME - t.start_time) * 24, 2),
           t.in_min   = round((vEND_TIME - t.start_time) * 24 * 60, 2)
     where t.dim_id = vDIM_ID
       and t.work_date = vWORK_DATE
       and t.end_time is null;
  
    commit;
  
  exception
    when others then
      null;
  end;

  /* Запись логов FACT */
  procedure pInsertFactLog(vFACT_ID in number, vWORK_DATE in date) is
  begin
  
    insert into SYSTEM_REFRESH_FACT_LOG t
      select vFACT_ID   fact_id,
             vWORK_DATE work_date,
             sysdate    log_date,
             null       log_msg,
             sysdate    start_time,
             null       end_time,
             null       is_error,
             null       in_hour,
             null       in_min
        from dual;
    commit;
  exception
    when others then
      null;
  end;

  /* Обновление логов FACT */
  procedure pUpdateFactLog(vFACT_ID   in number,
                           vWORK_DATE in date,
                           vLOG_MSG   in varchar2,
                           vEND_TIME  in date,
                           vIS_ERROR  in number) is
  begin
  
    update SYSTEM_REFRESH_FACT t
       set t.last_refresh_date_wd = vWORK_DATE,
           t.is_error             = vIS_ERROR,
           t.th_end_time          = sysdate
     where t.id = vFACT_ID;
  
    update SYSTEM_REFRESH_FACT_LOG t
       set t.log_date = sysdate,
           t.log_msg  = vLOG_MSG,
           t.end_time = vEND_TIME,
           t.is_error = vIS_ERROR,
           t.in_hour  = round((vEND_TIME - t.start_time) * 24, 2),
           t.in_min   = round((vEND_TIME - t.start_time) * 24 * 60, 2)
     where t.fact_id = vFACT_ID
       and t.work_date = vWORK_DATE
       and t.end_time is null;
  
    commit;
  
  exception
    when others then
      null;
  end;

  /* Обновление mviews */
  procedure refresh_mview(nstart_id number default -1, th in number) is
    d1 date;
    d2 date;
  
    cursor cr_mview is
      select ml.owner, ml.owner || '.' || ml.mview_name mview_name, m.thead
        from all_mviews ml, MVIEW_THEAD m
       where ml.OWNER = m.owner
         and ml.MVIEW_NAME = m.mview_name
         and m.thead = th
       order by m.id;
  
    vcr_mview cr_mview%rowtype;
  begin
  
    open cr_mview;
    loop
      fetch cr_mview
        into vcr_mview;
      exit when cr_mview%notfound;
    
      begin
      
        d1 := sysdate;
        dbms_mview.refresh(vcr_mview.mview_name,
                           'C',
                           atomic_refresh => false);
      
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => vcr_mview.mview_name,
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => vcr_mview.mview_name,
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    end loop;
    close cr_mview;
  end;

  /* Обновление mtable */
  procedure refresh_mtable(nstart_id number default -1,
                           wd        in date,
                           mtable    varchar2) is
    d1 date;
    d2 date;
  
  begin
  
    if mtable = 'MAIN.T_BALBLN' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.t_balbln z
         where z.fromdate = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.t_balbln
          select b.val_id,
                 b.dep_id,
                 b.id,
                 b.flzo,
                 b.planfl,
                 b.fromdate,
                 b.bal_in,
                 b.bal_out,
                 b.specfl,
                 b.natval_in,
                 b.natval_out
            from main.v_t_balbln b
           where b.fromdate = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.T_BALBLN',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.T_BALBLN',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    elsif mtable = 'MAIN.T_TRNDTLBLN' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.T_TRNDTLBLN z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.T_TRNDTLBLN
          select id,
                 nord,
                 incomfl,
                 val_id,
                 det_id,
                 dep_id,
                 acc_id,
                 doper,
                 sdok,
                 nat_sdok,
                 postfl,
                 flzo,
                 nord_hdr
            from main.V_T_TRNDTLBLN t
           where t.doper = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.T_TRNDTLBLN',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.T_TRNDTLBLN',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    elsif mtable = 'MAIN.T_BALANL' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.T_BALANL z
         where z.fromdate = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.T_BALANL
          select b.val_id,
                 b.dep_id,
                 b.id,
                 b.flzo,
                 b.planfl,
                 b.fromdate,
                 b.bal_in,
                 b.bal_out,
                 b.specfl,
                 b.natval_in,
                 b.natval_out
            from main.V_T_BALANL b
           where b.fromdate = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.T_BALANL',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.T_BALANL',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    elsif mtable = 'MAIN.T_TRNDTLANL' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.T_TRNDTLANL z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.T_TRNDTLANL
          select id,
                 nord,
                 incomfl,
                 val_id,
                 det_id,
                 dep_id,
                 acc_id,
                 doper,
                 sdok,
                 nat_sdok,
                 postfl,
                 flzo,
                 nord_hdr
            from main.V_T_TRNDTLANL t
           where t.doper = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.T_TRNDTLANL',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.T_TRNDTLANL',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.T_NUM' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.T_NUM z where z.fromdate = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.T_NUM
          select dep_id, id, planfl, fromdate, num_in, num_out, todate
            from main.V_T_NUM t
           where t.fromdate = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.T_NUM',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.T_NUM',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.L_DELAY_TURN_COLVIR' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.L_DELAY_TURN_COLVIR z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.L_DELAY_TURN_COLVIR
          select opr_id,
                 tra_id,
                 nord,
                 val_id,
                 dep_id,
                 acc_id,
                 doper,
                 sdok,
                 nat_sdok
            from main.v_l_delay_turn_colvir b
           where b.doper = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.L_DELAY_TURN_COLVIR',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.L_DELAY_TURN_COLVIR',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.L_DELAY_TURN_COLVIR_AGR' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.L_DELAY_TURN_COLVIR_AGR z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.L_DELAY_TURN_COLVIR_AGR
          select opr_id,
                 tra_id,
                 nord,
                 val_id,
                 dep_id,
                 acc_id,
                 doper,
                 sdok,
                 nat_sdok
            from main.v_L_DELAY_TURN_COLVIR_AGR b
           where b.doper = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.L_DELAY_TURN_COLVIR_AGR',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.L_DELAY_TURN_COLVIR_AGR',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.G_ACC_CASHFLOW' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.G_ACC_CASHFLOW z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.G_ACC_CASHFLOW
          select b.id,
                 b.acc_dep_id,
                 b.acc_id,
                 b.doper,
                 b.soper,
                 b.pg,
                 b.transfer_taxcode,
                 b.paytype,
                 b.knp,                 
                 b.frmt,
                 b.fr_code,
                 b.code_bcl,
                 b.tr_acc_code
            from main.V_G_ACC_CASHFLOW b
           where b.DOPER = wd;
      
        commit;
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.G_ACC_CASHFLOW',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.G_ACC_CASHFLOW',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.ENPF_TRNDTL' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.ENPF_TRNDTL z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.ENPF_TRNDTL
          select id,
                 dep_id,
                 acc_id,
                 clidep_id,
                 cli_id,
                 doper,
                 incomfl,
                 val_code,
                 sdok,
                 nps_code,
                 acc_code,
                 bank_code,
                 nps_code_corr,
                 acc_code_corr,
                 bank_code_corr,
                 cli_name_corr,
                 rnn_corr,
                 knp,
                 knp_name,
                 txt_dscr,
                 null app_id,
                 execdt
            from main.v_ENPF_TRNDTL m
           where m.DOPER = wd;
        commit;
      
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.ENPF_TRNDTL',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.ENPF_TRNDTL',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.ENPF_CH_TRNDTL' then
    
      d1 := sysdate;
      begin
      
        delete from /*+ PARALLEL(6) */ main.ENPF_CH_TRNDTL z
         where z.doper = wd;
        commit;
      
        insert /*+ APPEND PARALLEL(6) */
        into main.ENPF_CH_TRNDTL
          select id,
                 dep_id,
                 acc_id,
                 clidep_id,
                 cli_id,
                 doper,
                 incomfl,
                 val_code,
                 sdok,
                 nat_sdok,
                 nps_code,
                 acc_code,
                 bank_code,
                 nps_code_corr,
                 acc_code_corr,
                 bank_code_corr,
                 cli_name_corr,
                 rnn_corr,
                 knp,
                 knp_name,
                 txt_dscr,
                 null app_id,
                 execdt
            from main.v_ENPF_CH_TRNDTL m
           where m.DOPER = wd;
        commit;
      
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.ENPF_CH_TRNDTL',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.ENPF_CH_TRNDTL',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    elsif mtable = 'MAIN.TMP_L_DEAPCN' then
    
      d1 := sysdate;
      begin
      
        main.prefresh_ldeapcn(pdate => wd);
        commit;
      
        d2 := sysdate;
        pWriteMatViewLog(nstart_id   => nstart_id,
                         amviewname  => 'MAIN.TMP_L_DEAPCN',
                         alogmsg     => 'ok',
                         tstart_time => d1,
                         tstop_time  => d2,
                         fis_error   => 0);
      
      exception
        when others then
          begin
          
            d2 := sysdate;
            pWriteMatViewLog(nstart_id   => nstart_id,
                             amviewname  => 'MAIN.TMP_L_DEAPCN',
                             alogmsg     => 'ppc ' || sqlerrm,
                             tstart_time => d1,
                             tstop_time  => d2,
                             fis_error   => 1);
          end;
      end;
    
    end if;
  end;

  /*Обновление dimensions*/
  procedure refresh_dim(wd date, thread number) is
  
    l_sql_stmt varchar2(200);
    there_are_running_job exception;
  
  begin
  
    for rec in (select *
                  from system_refresh_dim z
                 where z.th = THREAD
                --where z.id = 38
                --where z.system_dim_area = 21
                 order by z.th, z.id) loop
    
      begin
        update system_refresh_dim d
           set d.th_begin_time        = sysdate,
               d.th_end_time          = null,
               d.last_refresh_date_wd = wd,
               d.is_error             = -1
         where d.id = rec.id;
        commit;
      end;
    
      declare
        l_sql_stmt varchar2(200);
      begin
      
        -- Execute the DML in parallel
        l_sql_stmt := 'begin ' || rec.procedure_name || '(to_date(''' ||
                      to_char(wd, 'dd.mm.yyyy') || ''', ''dd.mm.yyyy''), ' ||
                      rec.id || '); end;';
      
        execute immediate l_sql_stmt;
      
      exception
        when others then
          declare
            sqltxt varchar2(2000);
          begin
            sqltxt := 'ошибка при старте процедуры ' || sqlerrm;
          
            update system_refresh_dim f
               set f.th_end_time = sysdate, f.is_error = 2 -- ошибка при старте процедуры 
             where f.id = rec.id;
          
            /*insert into system_refresh_dim_log
              (system_refresh_dim, txt, dat, wd)
            values
              (rec.id, sqltxt, sysdate, wd);*/
            commit;
          
          end;
        
      end;
    
    end loop;
  
  end refresh_dim;

  /*Обновление custom*/
  procedure refresh_custom(wd date, thread number) is
    l_sql_stmt varchar2(200);
    there_are_running_job exception;
  
  begin
  
    for rec in (select *
                  from system_refresh_custom z
                 where z.th = THREAD
                 order by z.id) loop
    
      begin
        update system_refresh_custom d
           set d.th_begin_time = sysdate, d.th_end_time = null
         where d.id = rec.id;
        commit;
      end;
    
      DECLARE
        l_sql_stmt varchar2(200);
      BEGIN
      
        -- Execute the DML in parallel
        l_sql_stmt := 'begin ' || rec.procedure_name || '(to_date(''' ||
                      to_char(wd, 'dd.mm.yyyy') || ''', ''dd.mm.yyyy''), ' ||
                      rec.id || '); end;';
      
        execute immediate l_sql_stmt;
      
      exception
        when others then
          declare
            sqltxt varchar2(2000);
          begin
            sqltxt := 'ошибка при старте процедуры ' || sqlerrm;
            update system_refresh_custom f
               set f.th_end_time = sysdate, f.is_error = 1
             where f.id = rec.id;
          
            insert into system_refresh_custom_log
              (system_refresh_custom, txt, dat, wd)
            values
              (rec.id, sqltxt, sysdate, wd);
            commit;
          end;
        
      END;
    
    end loop;
  
  end refresh_custom;

  /*Откат измерении*/
  procedure rollback_dim(vt in varchar2, wd date default p_operday) is
  
    p_table_name varchar2(100);
    l_sql_stmt   varchar2(2000);
    there_are_running_job exception;
    ex                    exception;
  
  begin
  
    /*if wd <> p_operday then
      raise ex;
    end if;*/
  
    for rec in (select *
                  from system_refresh_dim z
                 where /*z.th between 1 and 7
                                                   and */
                 z.TABLE_NAME = nvl(vt, z.TABLE_NAME)
             and z.REFRESH_AREA = 9
             and z.TABLE_NAME not in ('G_WORKING_DATE_D',
                                      'G_CALENDAR_D',
                                      'L_OVERDUE_D',
                                      'RM_EQUITY_D',
                                      'RM_MARGIN_D',
                                      'RM_PRUDENT_D',
                                      'RM_RTG_D',
                                      'PB_PAYMENT_D',
                                      'CH_PAYMENT_D',
                                      'L_DEAPOST_SD',
                                      'G_ACCSPEC_D',
                                      'OCS_CALL_D',
                                      'OCS_ENDPOINT_D',
                                      'OCS_PARTY_D',
                                      'CRB_REPORT_D',
                                      'OCS_AGENT_D',
                                      
                                      'OCS_PARTYSTAT_D',
                                      'OCS_USERDATA_D',
                                      'OCS_USERDATA_D',
                                      'CCP_VOICE_A_HOUR_D',
                                      'CCP_202_COMPSTAT_D',
                                      'CCP_ROUTEPOINT_HOUR_D',
                                      'CCP_VOICE_RP_HOUR_D',
                                      'CCP_OBJECT_D',
                                      'CCP_TIME_D')
                 order by z.th desc, z.id desc) loop
    
      p_table_name := rec.table_name;
    
      DECLARE
        l_sql_stmt varchar2(2000);
      BEGIN
      
        -- Execute the DML in parallel
        l_sql_stmt := 'begin delete from ' || rec.table_name ||
                      ' where dt_begin = to_date(''' ||
                      to_char(wd, 'dd.mm.yyyy') ||
                      ''', ''dd.mm.yyyy''); commit;' || ' update ' ||
                      rec.table_name || ' set dt_end = to_date(''' ||
                      to_char(ETL_MAIN.GET_C_END_DATE, 'dd.mm.yyyy') ||
                      ''', ''dd.mm.yyyy'')' || ' where dt_end = to_date(''' ||
                      to_char(wd - 1, 'dd.mm.yyyy') ||
                      ''', ''dd.mm.yyyy''); commit; ' || 'end;';
      
        --dbms_output.put_line(l_sql_stmt);
        execute immediate l_sql_stmt;
      
      exception
        when others then
          declare
            sqltxt varchar2(2000);
          begin
            dbms_output.put_line(p_table_name);
            dbms_output.put_line(sqlerrm);
            dbms_output.put_line(dbms_utility.format_error_backtrace);
          end;
      END;
    end loop;
  
  end rollback_dim;

  /*Мониторинг измерении*/
  procedure monitoring_dim(vwd date default p_operday) is
  
    rec_loader ETL_MAIN.REC_TYPE;
    vSQL       Varchar2(2000);
    cnt        number := 0;
    i          number := 0;
  
  begin
  
    begin
      select count(1)
        into i
        from SYSTEM_MONITORING_DIM
       where work_date = vwd;
    exception
      when others then
        i := 0;
    end;
  
    if nvl(i, 0) = 0 /*and vwd = p_operday*/
     then
    
      for rec in (select distinct tl.work_date,
                                  t.th,
                                  t.id,
                                  t.table_name,
                                  t.th_begin_time,
                                  t.th_end_time,
                                  round((th_end_time - th_begin_time) * 24 * 60,
                                        2) in_min,
                                  t.is_error
                    from SYSTEM_REFRESH_DIM t, SYSTEM_REFRESH_DIM_LOG tl
                   where t.id = tl.dim_id
                     and t.th > 0
                     and tl.work_date = vwd
                     and t.refresh_area not in (11, 15, 21, 25)
                     and t.table_name not in
                         ('G_WORKING_DATE_D',
                          'G_CALENDAR_D',
                          'L_OVERDUE_D',
                          'G_ACCSPEC_D',
                          'SS_VAR_TARGETS_D',
                          'SS_RDDEASHD_SD',
                          'L_DEAPOST_SD',
                          'SS_BNKDEALSHD_SD',
                          'IB_HISTORY_SD',
                          'ENPF_HISTORYAPP_SD',
                          'ENPF_TRANSFER_SD')
                   order by t.th, t.id) loop
      
        vSQL := 'select count(1)
                     from ' || rec.table_name || ' a
                     where a.dt_begin = :1';
        begin
          open rec_loader for vSQL
            using rec.work_date;
          fetch rec_loader
            into cnt;
          close rec_loader;
        
          insert into SYSTEM_MONITORING_DIM
            select rec.work_date,
                   rec.th,
                   rec.id,
                   rec.table_name,
                   cnt,
                   rec.th_begin_time,
                   rec.th_end_time,
                   rec.in_min,
                   rec.is_error
              from dual;
          commit;
        exception
          when others then
            null;
        end;
      
      end loop;
    end if;
  
  end monitoring_dim;

  /*Обновление facts*/
  PROCEDURE refresh_fact(wd date, thread number)
  
   IS
    l_sql_stmt varchar2(200);
    there_are_running_job exception;
  
  begin
  
    for rec in (select *
                  from system_refresh_fact z
                 where z.th = THREAD
                --where z.id = 11
                 order by ID) loop
    
      begin
        update system_refresh_fact d
           set d.th_begin_time        = sysdate,
               d.th_end_time          = null,
               d.last_refresh_date_wd = wd,
               d.is_error             = -1
         where d.id = rec.id;
      end;
    
      DECLARE
        l_sql_stmt varchar2(200);
      BEGIN
      
        -- Execute the DML in parallel
        l_sql_stmt := 'begin ' || rec.procedure_name || '(to_date(''' ||
                      to_char(wd, 'dd.mm.yyyy') || ''', ''dd.mm.yyyy''), ' ||
                      rec.id || '); end;';
      
        execute immediate l_sql_stmt;
      
      exception
        when others then
          declare
            sqltxt varchar2(2000);
          begin
            sqltxt := 'ошибка при старте процедуры ' || sqlerrm;
            update system_refresh_fact f
               set f.th_end_time = sysdate, f.is_error = 2
             where f.id = rec.id;
          
            /*insert into system_refresh_fact_log
              (system_refresh_fact, txt, dat, wd)
            values
              (rec.id, sqltxt, sysdate, wd);*/
            commit;
          end;
        
      END;
    
    end loop;
  
  end refresh_fact;

  /* Сбор статистики */
  procedure gather_stat(aTableName In Varchar2 default null,
                        aPartName  In Varchar2 default null,
                        aDate      In Date) is
    vPart Varchar2(3);
  
  begin
    dbms_application_info.set_client_info('Gather Stat: ' ||
                                          to_char(aDate, 'dd.mm.yyyy'));
  
    if aTableName is null then
      -- Простые таблицы
      for gs_rec in (select a.table_name
                       from user_tables a
                      where a.partitioned = 'NO'
                     --and a.table_name not like 'T%'
                     --and a.table_name not like '%STAGE%'
                      order by a.table_name) loop
        begin
          --dbms_stats.delete_table_stats(ownname => 'DWH', tabname => gs_rec.table_name);
          dbms_stats.gather_table_stats(ownname => 'DWH',
                                        tabname => gs_rec.table_name);
        exception
          when others then
            null;
        end;
      end loop;
    
      -- Секционированные таблицы
      if aPartName is not null then
        for gs_rec in (select a.table_name
                         from user_tables a
                        where a.partitioned = 'YES'
                       --and a.table_name not in ('CR_CRDACC','DS_DPSACC','CR_CRDSTS','DS_DPSSTS')
                        order by a.table_name) loop
          begin
            dbms_stats.gather_table_stats(ownname     => 'DWH',
                                          tabname     => gs_rec.table_name,
                                          partname    => aPartName,
                                          granularity => 'PARTITION');
          exception
            when others then
              null;
          end;
        end loop;
      end if;
    else
      begin
        select ut.partitioned
          into vPart
          from user_tables ut
         where ut.table_name = aTableName;
      
        if ((vPart = 'YES') and (aPartName is not null)) then
          dbms_stats.gather_table_stats(ownname     => 'DWH',
                                        tabname     => aTableName,
                                        partname    => aPartName,
                                        granularity => 'PARTITION');
          --dbms_stats.gather_table_stats(ownname => 'DWH', tabname => aTableName, estimate_percent => DBMS_STATS.AUTO_SAMPLE_SIZE, granularity => 'AUTO');
        else
          dbms_stats.gather_table_stats(ownname => 'DWH',
                                        tabname => aTableName);
        end if;
      exception
        when others then
          null;
      end;
    end if;
  end;

  procedure start_etl is
    start_id number;
    aDate    date := p_operday;
    res      integer;
  begin
  
    --refresh_mview(start_id, 0);
    --refresh_mview(start_id, 10);
    --refresh_mview(start_id, 20);
    --refresh_mview(start_id, 30);
    --refresh_mview(start_id, 40);
  
    /*res := xx_mail.send_mail(p_host => '172.22.0.143',
    p_dom => '',
    p_to => 'musirep.d@hcsbk.kz, dauletali.mussirep@gmail.com',
    p_subject => 'Оповещение новогодней загрузки 1',
    p_text => 'Matviews refreshed');   */
  
    --refresh_dim(aDate, 0);
    refresh_dim(aDate, 1);
    refresh_dim(aDate, 2);
    refresh_dim(aDate, 3);
    refresh_dim(aDate, 4);
    refresh_dim(aDate, 5);
    refresh_dim(aDate, 6);
  
    /*res := xx_mail.send_mail(p_host => '172.22.0.143',
    p_dom => '',
    p_to => 'musirep.d@hcsbk.kz, dauletali.mussirep@gmail.com',
    p_subject => 'Оповещение новогодней загрузки 2',
    p_text => 'Dimensions refreshed');     */
  
    refresh_fact(aDate, 1);
    refresh_fact(aDate, 2);
  
    /*res := xx_mail.send_mail(p_host => '172.22.0.143',
    p_dom => '',
    p_to => 'musirep.d@hcsbk.kz, dauletali.mussirep@gmail.com',
    p_subject => 'Оповещение новогодней загрузки 3',
    p_text => 'Facts refreshed');  */
  
  exception
    when others then
      /*res := xx_mail.send_mail(p_host => '172.22.0.143',
      p_dom => '',
      p_to => 'musirep.d@hcsbk.kz, dauletali.mussirep@gmail.com',
      p_subject => 'Оповещение новогодней загрузки error',
      p_text => sqlerrm||' '||dbms_utility.format_error_backtrace);*/
      null;
  end;

begin
  null;
end ETL_MAIN;
/
