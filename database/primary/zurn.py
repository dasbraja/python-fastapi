from config import IO
from logger import *

def get_aggregate_activation(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):

    where1 = ""
    where2 = ""
    if deveui:
        where2 += f" and a.deveui=%(deveui)s "

    elif devicetype:
        where2 += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where1 += f" and  str_to_date(substring(utime,1, 10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where1 += f" and  str_to_date(substring(utime,1, 10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""
            select a.devicetype, a.deveui, coalesce(b.alert_count,0) alert_count, 
            coalesce(avg_activation_count, 0) daily_avg_activation_count,
            coalesce(tot_activation_count, 0) tot_activation_count
            from devicemap a
            left join 
            (
                select deveui, count(distinct num_uses_tot) alert_count
                from zurnfaucet 
                where alertstatus='sent'
                {where1}
                group by deveui
                union all
                select deveui, count(distinct num_uses_tot)alert_count
                from zurnflush                
                where alertstatus='sent'
                {where1}
                group by deveui
            ) b
            on a.deveui = b.deveui
            left join 
            (
                select a.deveui, cast(avg(activation_count) as UNSIGNED) avg_activation_count, cast(sum(activation_count) as UNSIGNED) tot_activation_count
                    from 
                    (
                        select deveui, substring(utime,1, 10) eventdate,     
                        (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 activation_count
                        from zurnfaucet 
                        where dayofweek(substring(utime,1, 10)) in (2,3,4,5,6)
                        {where1}
                        group by deveui, substring(utime,1, 10)   
                        having (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 >15
                        union all
                        select deveui, substring(utime,1, 10) eventdate,     
                        (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 activation_count
                        from zurnflush
                        where dayofweek(substring(utime,1, 10)) in (2,3,4,5,6)
                        {where1}
                        group by deveui, substring(utime,1, 10)
                        having (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 >15
                    ) a
                group by a.deveui
            ) c
            on a.deveui = c.deveui
            where devicetype in ('Faucet', 'Flush Valve')
            {where2}
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'alert_count': i[2],
                'daily_avg_activation_count': i[3],
                'tot_activation_count': i[4]
            })

    return ret


def get_daily_aggregate_activation(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""select b.devicetype, a.deveui, a.eventdate, a.activation_count
                from 
                (
                    select deveui, substring(utime,1, 10) eventdate,     
                    (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 activation_count
                    from zurnfaucet 
                    group by deveui, substring(utime,1, 10)        
                    union all
                    select deveui, substring(utime,1, 10) eventdate,     
                    (max(cast(num_uses_tot as UNSIGNED)) - min(cast(num_uses_tot as UNSIGNED))) +1 activation_count
                    from zurnflush
                    group by deveui, substring(utime,1, 10)
                ) a
                left join devicemap b
                on a.deveui = b.deveui
                where 1=1
                {where}
                order by a.eventdate , a.deveui 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'activation_count': i[3]
            })

    return ret


def get_alert_status(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"




    query = f"""select distinct devicetype, deveui, gatewayid, eventtime, eventdate, message_type, num_uses_tot, alertstatus 
    from 
    (
    select 'faucet' devicetype, deveui, gatewayid, utime eventtime, substring(utime,1, 10) eventdate, message_type, num_uses_tot, alertstatus 
    from zurnfaucet 
    where alertstatus='sent'
    union all
    select 'flush' devicetype, deveui, gatewayid, utime eventtime, substring(utime,1, 10) eventdate, message_type, num_uses_tot, alertstatus 
    from zurnflush
    where alertstatus='sent'
    ) a
    where alertstatus='sent'
    {where}
    order by eventdate 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'eventtime': i[3],
                'eventdate': i[4],
                'message_type': i[5],
                'num_uses_tot': i[6],
                'alertstatus': i[7],
            })
    return ret


def get_device_alert_status(io: IO, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and deveui=%(deveui)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""select distinct devicetype, deveui, gatewayid, eventtime, eventdate, message_type, num_uses_tot, alertstatus 
    from 
    (
    select 'faucet' devicetype, deveui, gatewayid, utime eventtime, substring(utime,1, 10) eventdate, message_type, num_uses_tot, alertstatus 
    from zurnfaucet 
    where alertstatus='sent'
    union all
    select 'flush' devicetype, deveui, gatewayid, utime eventtime, substring(utime,1, 10) eventdate, message_type, num_uses_tot, alertstatus 
    from zurnflush
    where alertstatus='sent'
    ) a
    where alertstatus='sent'
    {where}
    order by eventdate 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'deveui': deveui}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'eventtime': i[3],
                'eventdate': i[4],
                'message_type': i[5],
                'num_uses_tot': i[6],
                'alertstatus': i[7],
            })
    return ret


def get_events_timeseries(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    where2 = ""
    if deveui:
        where1 += f" and deveui=%(deveui)s "
    elif devicetype:
        where2 += f" and b.devicetype=%(devicetype)s "

    if eventdate_from:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"




    query = f"""select b.devicetype, a.deveui, a.num_uses_tot, a.utime, a.diff_minutes
                from
                (
                    select deveui,  num_uses_tot, cast(cast(utime as datetime) as char) utime,                        
                    TIMESTAMPDIFF(MINUTE,  cast(coalesce(lag(utime) over (partition by deveui order by num_uses_tot), utime) as datetime), cast(utime as datetime)) diff_minutes
                    from zurnflush  
                    where 1=1
                    {where1}           
                    union
                    select deveui,  num_uses_tot, cast(cast(utime as datetime) as char) utime,                        
                    TIMESTAMPDIFF(MINUTE,  cast(coalesce(lag(utime) over (partition by deveui order by num_uses_tot), utime) as datetime), cast(utime as datetime)) diff_minutes
                    from zurnfaucet   
                    where 1=1
                    {where1}      
                ) a    
                left join devicemap b
                on a.deveui = b.deveui                
                where a.diff_minutes >0  and a.diff_minutes < 360
                {where2}   
                order by a.deveui, utime    
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'num_uses_tot': i[2],
                'utime': i[3],
                'diff_minutes': i[4]
            })
    return ret
