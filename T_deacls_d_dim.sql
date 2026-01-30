create or replace procedure T_DEACLS_D_DIM(wd                    date,
                                           system_refresh_dim_id number default 10) is

  ex exception;
  sqltxt varchar2(2000);

  vtrans_count Number(4) := 0;
  i            Number;

  cursor v_cls is
    select *
      from (select id        source_id,
                   id_hi     source_id_hi,
                   code, 
                   groupfl, 
                   nlevel, 
                   arcfl, 
                   arestfl, 
                   longname, 
                   prim, 
                   refname
              from main.T_DEACLS
            minus
            select source_id, 
                   source_id_hi, 
                   code, 
                   groupfl, 
                   nlevel, 
                   arcfl, 
                   arestfl, 
                   longname, 
                   prim, 
                   refname
              from T_DEACLS_D tt
             where wd between tt.dt_begin and tt.dt_end);
  v_cls_rec v_cls%rowtype;

begin
  
  ETL_MAIN.pInsertDimLog(system_refresh_dim_id, wd);

  if wd <> p_operday then
    raise ex;
  end if;

  open v_cls;
  loop
    fetch v_cls
      into v_cls_rec;
    exit when v_cls%notfound;
  
    if vtrans_count = ETL_MAIN.GET_C_COMMIT_COUNT then
      vtrans_count := 0;
      commit;
    end if;
  
    select count(a.id)
      into i
      from T_DEACLS_D a
     where a.source_id = v_cls_rec.source_id
       and a.dt_end = ETL_MAIN.GET_C_END_DATE;
  
    if i > 0 then
      begin
        update T_DEACLS_D a
           set a.dt_end = wd - 1
         where a.source_id = v_cls_rec.source_id
           and a.dt_end = ETL_MAIN.GET_C_END_DATE;
      end;
    end if;
  
    insert into T_DEACLS_D
      (id, 
       id_hi, 
       dt_begin, 
       dt_end, 
       source_id, 
       source_id_hi, 
       code, 
       groupfl, 
       nlevel, 
       arcfl, 
       arestfl, 
       longname, 
       prim, 
       refname)
    values
      (T_DEACLS_D_SEQ.Nextval,
       null,
       wd,
       ETL_MAIN.GET_C_END_DATE,
       v_cls_rec.source_id, 
       v_cls_rec.source_id_hi, 
       v_cls_rec.code, 
       v_cls_rec.groupfl, 
       v_cls_rec.nlevel, 
       v_cls_rec.arcfl, 
       v_cls_rec.arestfl, 
       v_cls_rec.longname, 
       v_cls_rec.prim, 
       v_cls_rec.refname);
  
    vtrans_count := vtrans_count + 1;
  
  end loop;
  close v_cls;

  -- update id_hi
  update T_DEACLS_D d
     set d.id_hi =
         (select d1.id
            from T_DEACLS_D d1
           where d1.source_id = d.source_id_hi
             and wd between d1.dt_begin and d1.dt_end)
   where d.id_hi is null
     and wd between d.dt_begin and d.dt_end;

  if vtrans_count > 0 then
    commit;
  end if;

  ETL_MAIN.pUpdateDimLog(system_refresh_dim_id, wd, 'no_error', sysdate, 0);

exception
  when others then
    sqltxt := sqlerrm;
  
    ETL_MAIN.pUpdateDimLog(system_refresh_dim_id, wd, sqltxt, sysdate, 1);
      
end T_DEACLS_D_DIM;
