---三个版本的头条数据
SELECT
    d.tt_cid, 
    d.appversioncode,
    table_freq.freq,
    -- d.tt_type,
    -- table_show.tt_video_type,
    --d.tt_title,
    --d.tt_dec,
    --d.tt_btn,
    d.tt_apk_name,
    d.tt_apk_pkg,
    -- d.tt_apk_size,
    -- d.tt_apk_url,
    table_show.total_show,
    table_click.total_click,
    table_download_start.total_download_start,
    table_download_finish.total_download_finish,
    table_download_fail.total_download_fail,
    table_on_install.total_on_install,
    table_on_install_already.total_on_install_already
    -- d.tt_video_url,
    -- d.tt_video_md5
FROM (
    select * from (
    select
      *,
      ROW_NUMBER() OVER(PARTITION BY tt_cid ORDER BY 1 DESC) rn
    from metaapp0.log_distribution WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
) d
LEFT JOIN (
    SELECT tt_cid, count(tt_cid) as freq
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_distribution WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid
) as table_freq
ON table_freq.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, tt_video_type, count(*) as total_show 
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_show WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid, tt_video_type
) as table_show
ON table_show.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(*) as total_click
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_click WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid
) as table_click
ON table_click.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(*) as total_download_start 
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_download_start WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid
) as table_download_start
ON table_download_start.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(*) as total_download_finish 
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_download_finish WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid
) as table_download_finish
ON table_download_finish.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(*) as total_download_fail 
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_download_fail WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid 
) as table_download_fail
ON table_download_fail.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(1) as total_on_install
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_install WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}') AND show_id > 0
    ) t where rn = 1
    GROUP BY tt_cid 
) as table_on_install
ON table_on_install.tt_cid = d.tt_cid
LEFT JOIN (
    SELECT tt_cid, count(1) as total_on_install_already 
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_install WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}') AND show_id = 0
    ) t where rn = 1
    GROUP BY tt_cid 
) as table_on_install_already
ON table_on_install_already.tt_cid = d.tt_cid
ORDER BY freq DESC
limit 10000;




---三个版本的头条数据
SELECT
    d.tt_cid, 
    table_freq.freq,
    d.tt_extra,
    -- d.tt_type,
    -- table_show.tt_video_type,
    --d.tt_title,
    --d.tt_dec,
    --d.tt_btn,
    d.tt_apk_name,
    d.tt_apk_pkg,
    -- d.tt_apk_size,
    -- d.tt_apk_url,
    table_show.total_show,
    table_click.total_click,
    table_download_start.total_download_start,
    table_download_finish.total_download_finish,
    table_download_fail.total_download_fail,
    table_on_install.total_on_install,
    table_on_install_already.total_on_install_already
    -- d.tt_video_url,
    -- d.tt_video_md5
FROM (
    select * from (
    select
      *,
      ROW_NUMBER() OVER(PARTITION BY tt_cid ORDER BY 1 DESC) rn
    from metaapp0.log_distribution WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
) d
LEFT JOIN (
    SELECT tt_cid, count(tt_cid) as freq
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_distribution WHERE ds = '${DATE_STR}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) t where rn = 1
    GROUP BY tt_cid
) as table_freq
ON table_freq.tt_cid = d.tt_cid







SELECT  a.tt_apk_pkg, a.tt_apk_name,c.unit_id,
    count(1) show_cnt
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_on_show WHERE ds >= '${DATE_START}' and ds<='${DATE_END}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) a 
LEFT JOIN (
    SELECT tt_request_id, GET_JSON_OBJECT(tt_extra,'$.rit') as unit_id
    FROM (
        select *, ROW_NUMBER() OVER(PARTITION BY kind_event_id ORDER BY 1 DESC) rn from metaapp0.log_distribution WHERE ds >= '${DATE_START}' and ds<='${DATE_END}' AND app_version_code in( '${APP_VERSION_1}','${APP_VERSION_2}','${APP_VERSION_3}')
    ) b where rn = 1 and unit_id = '${unit_id}' ) c
ON a.tt_request_id = c.tt_request_id
where a.rn = 1 and c.unit_id is not null
group by a.tt_apk_pkg, a.tt_apk_name,c.unit_id
order by show_cnt desc
limit 10000;