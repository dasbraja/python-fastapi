from config import IO
from logger import *

def get_battery_alert(io: IO, devicetype: None, deveui: None, alertname: None, alertstatus: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if alertname:
        where += f" and rparam=%(alertname)s "

    if alertstatus:
        where += f" and rdata=%(alertstatus)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""select distinct b.devicetype, a.deveui, gatewayid, recordnumber, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, rdata alertstatus
        from kafkaparsed a
        left join
        devicemap b
        on a.deveui = b.deveui   
        where 1=1      
        and rparam in ('Battery State Change')
        {where}
        order by eventdate desc, recordnumber desc 
                """
    ret = []
    log.info(query)

    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'alertname': alertname,
                                      'alertstatus': alertstatus, 'eventdate_from': eventdate_from,
                                      'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'recordnumber': i[3],
                'eventtime': i[4],
                'eventdate': i[5],
                'alertname': i[6],
                'alertstatus': i[7],
            })
    return ret


def get_device_battery_alert(io: IO, deveui: None, alertname: None, alertstatus: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "

    if alertname:
        where += f" and rparam=%(alertname)s "

    if alertstatus:
        where += f" and rdata=%(alertstatus)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""select distinct b.devicetype, a.deveui, gatewayid, recordnumber, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, rdata alertstatus
    from kafkaparsed a
    left join
    devicemap b
    on a.deveui = b.deveui 
    where 1=1    
    and rparam in ('Battery State Change')
    {where}
    order by recordnumber desc, eventdate desc
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'deveui': deveui, 'alertname': alertname, 'alertstatus': alertstatus, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'recordnumber': i[3],
                'eventtime': i[4],
                'eventdate': i[5],
                'alertname': i[6],
                'alertstatus': i[7],
            })
    return ret



def get_alert_status(io: IO, devicetype: None, deveui: None, alertname: None, alertstatus: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if alertname:
        where += f" and rparam=%(alertname)s "

    if alertstatus:
        where += f" and alertstatus=%(alertstatus)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""select distinct b.devicetype, a.deveui, gatewayid, recordnumber, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, alertstatus
    from kafkaparsed a
    left join
    devicemap b
    on a.deveui = b.deveui 
    where alertstatus in ('resolved', 'sent')
    and rparam in ('Product Low Alert')
    {where}
    order by eventdate desc, recordnumber desc 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'alertname': alertname, 'alertstatus': alertstatus, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'recordnumber': i[3],
                'eventtime': i[4],
                'eventdate': i[5],
                'alertname': i[6],
                'alertstatus': i[7],
            })
    return ret

def get_device_alert_status(io: IO, deveui: None, alertname: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "

    if alertname:
        where += f" and rparam=%(alertname)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""select distinct b.devicetype, a.deveui, gatewayid, recordnumber, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, alertstatus
    from kafkaparsed a
    left join
    devicemap b
    on a.deveui = b.deveui 
    where alertstatus in ('resolved', 'sent')
    and rparam in ('Product Low Alert')
    {where}
    order by recordnumber desc, eventdate desc
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'deveui': deveui, 'alertname': alertname, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'gatewayid': i[2],
                'recordnumber': i[3],
                'eventtime': i[4],
                'eventdate': i[5],
                'alertname': i[6],
                'alertstatus': i[7],
            })
    return ret


def get_aggregate_product(io: IO, devicetype: None, deveui: None, roll: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    if deveui:
        where1 += f" and a.deveui=%(deveui)s "
    elif devicetype:
       where1 += f" and devicetype=%(devicetype)s "

    where2 =""
    if eventdate_from:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    where3 = ""
    if roll:
        where3 += f" and a.roll=%(roll)s "

    query = f"""
    select a.devicetype, a.deveui, coalesce(b.resolved_count,0) alert_resolved_count, 
    coalesce(b.sent_count,0) alert_sent_count, 
    coalesce(c.product_usage,0) product_usage, 
    coalesce(d.dispense_count, 0) dispense_count
    from devicemap a
    left join 
    (
    select deveui, alertname, 
    sum(resolved_status) resolved_count,
    sum(sent_status) sent_count
    from (
		select  distinct deveui, rparam alertname,  recordnumber, alertstatus ,  
		case when alertstatus = 'resolved' then 1 else 0  end resolved_status,
		case when alertstatus = 'sent' then 1 else 0  end sent_status,
		case when rdata='Roll 0' then 0 
        when rdata='Roll 1' then 1
        else 99
        end roll
		from kafkaparsed a
		where alertstatus in ('resolved', 'sent')
		and rparam in ('Product Low Alert')
		{where2}
    ) a
    where 1=1
    {where3}
    group by deveui, alertname      
    ) b
    on a.deveui = b.deveui
    left join 
    (
     select  distinct deveui, cast(coalesce(sum(diff)/100,'0') as DECIMAL(4,3)) product_usage
        from
        (
            select deveui, gatewayid, utime, timestamp,  remaining,recordnumber,
            coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) last_remaining,
        case
        when
        cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED) between 0 and 15 then
        cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED)
        else 0
        end
        diff,
            cmd, param
        from kafkaparsed 
        where rparam = 'Product Fuel Gauge'
        {where2}            
        ) a
        group by deveui
                
     ) c
     on a.deveui = c.deveui
    
    left join 
    (
        select deveui, count(distinct recordnumber) dispense_count
            from kafkaparsed             
            where rparam ='DISPENSE'
            {where2}
            group by deveui
    ) d
    on a.deveui = d.deveui
    where a.devicetype in ('Soap Dispenser', 'Paper Towel Dispenser', 'Toilet Tissue Dispenser')
    {where1} 
            """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'roll': roll, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'alert_resolved_count': i[2],
                'alert_sent_count': i[3],
                'product_usage': i[4],
                'dispense_count': i[5]
            })
    return ret


def get_daily_productalert_summary(io: IO, devicetype: None, deveui: None, roll: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    where2 = ""
    if deveui:
        where1 += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where1 += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    if roll:
        where2 += f" and roll=%(roll)s "


    query = f"""
    select devicetype, deveui, eventdate, alertname, roll,    
    sum(roll0_resolved_status)  + sum(roll0_sent_status) roll0_alert_count,
    sum(roll1_resolved_status)  + sum(roll1_sent_status) roll1_alert_count,
    sum(roll0_resolved_status) roll0_resolved_count,
    sum(roll1_resolved_status) roll1_resolved_count,
    sum(roll0_sent_status) roll0_sent_count,
    sum(roll1_sent_status) roll1_sent_count
    from (
		select distinct b.devicetype, a.deveui, a.recordnumber, substring(utime,1,10) eventdate, rparam alertname,  alertstatus , 1 any_alert_status,
		case when rdata='Roll 0' then 0 
        when rdata='Roll 1' then 1
        else 99
        end roll,    		
		case when alertstatus = 'resolved' and rdata= 'Roll 0' then 1 else 0  end roll0_resolved_status,
        case when alertstatus = 'resolved' and rdata= 'Roll 1' then 1 else 0  end roll1_resolved_status,
		case when alertstatus = 'sent' and rdata= 'Roll 0' then 1 else 0  end roll0_sent_status,
        case when alertstatus = 'sent' and rdata= 'Roll 1' then 1 else 0  end roll1_sent_status
		from kafkaparsed a
		left join
		devicemap b
		on a.deveui = b.deveui 
		where alertstatus in ('resolved', 'sent')
		and rparam in ('Product Low Alert')
		{where1}
  
    ) a
    where 1=1
    {where2}
    group by devicetype, deveui, eventdate, alertname,roll         
    order by eventdate, deveui
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'roll': roll, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'alertname': i[3],
                'roll': i[4],
                'roll0_alert_count': i[5],
                'roll1_alert_count': i[6],
                'roll0_resolved_count': i[7],
                'roll1_resolved_count': i[8],
                'roll0_sent_count': i[9],
                'roll1_sent_count': i[10]
            })
    return ret

def get_daily_productusage_summary(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""
        select devicetype, restroom_location, store, deveui, eventdate, 
        case when devicetype='Soap Dispenser' then 0 else sum(roll0_product_usage) end
        roll0_product_usage, 
        case when devicetype='Soap Dispenser' then 0 else sum(roll1_product_usage) end roll1_product_usage,          
        sum((roll1_product_usage + roll0_product_usage)) product_usage 
        from
        (
        
        select distinct c.devicetype, c.restroom_location, c.store, a.deveui, roll, a.eventdate, 
        case when roll = '0 ' then cast(coalesce(a.rollusage,'0') as DECIMAL(4,3)) else 0
        end roll0_product_usage,
        case when roll = '1 ' then cast(coalesce(a.rollusage,'0') as DECIMAL(4,3)) else 0
        end roll1_product_usage
        from (
             select substring(utime,1, 10) eventdate, deveui, roll, sum(diff)/100 rollusage
                from
                (
                    select deveui, gatewayid, utime, timestamp,  remaining, recordnumber, roll,
                    coalesce(lead(remaining) over (partition by deveui, roll order by recordnumber), remaining) last_remaining,
                case
                when
                cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui, roll order by recordnumber), remaining) as UNSIGNED) between 0 and 15 then
                cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui, roll order by recordnumber), remaining) as UNSIGNED)
                else 0
                end
                diff,
                    cmd, param
                from kafkaparsed 
                where rparam = 'Product Fuel Gauge'            
                ) a
                group by substring(utime,1, 10), deveui, roll
                ) a
            left join devicemap c
            on a.deveui = c.deveui    
            where 1=1
            {where}
        ) x
        group by devicetype, restroom_location, store, deveui, eventdate
        order by eventdate , deveui
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'restroom_location': i[1],
                'store': i[2],
                'deveui': i[3],
                'eventdate': i[4],
                'roll0_product_usage': i[5],
                'roll1_product_usage': i[6],
                'product_usage': i[7]
            })
    return ret


def get_daily_dispense_summary(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""
        select c.devicetype, a.deveui, a.eventdate, a.dispense_count
        from (
        select deveui, substring(utime,1, 10) eventdate, count(distinct recordnumber) dispense_count
            from kafkaparsed             
            where rparam ='DISPENSE'
            group by deveui, substring(utime,1, 10)
        ) a
        left join devicemap c
        on a.deveui = c.deveui    
        where 1=1
        {where}
        order by a.eventdate, a.deveui            
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'dispense_count': i[3]
            })
    return ret

def get_daily_batteryalert_summary(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""

    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and b.devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""
       select devicetype, deveui, eventdate, sum(normal_status) normal_count, sum(low_status) low_count, sum(shutdown_status) shutdown_count
        from
        (
        select distinct b.devicetype, a.deveui, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, rdata alertstatus,
        case when rdata = 'Normal' then 1 else 0 end normal_status,
        case when rdata = 'Low' then 1 else 0 end low_status,
        case when rdata = 'Shutdown' then 1 else 0 end shutdown_status
        from kafkaparsed a                        
        left join
        devicemap b
        on a.deveui = b.deveui   
        where rparam in ('Battery State Change')    
        {where}                      
        ) a        
        group by devicetype, deveui, eventdate
        order by deveui , eventdate 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'normal_count': i[3],
                'low_count': i[4],
                'shutdown_count': i[5]
            })
    return ret


def get_battery_card(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    where2 = ""

    if deveui:
        where2 += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where2 += f" and a.devicetype=%(devicetype)s "

    if eventdate_from:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where1 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""
       select a.devicetype, a.deveui, 
        coalesce(sum(normal_status),0) normal_count,
        coalesce(sum(low_status),0) low_count,
        coalesce(sum(shutdown_status),0) shutdown_count
        from devicemap a
        left join 
        (
            select distinct a.deveui, utime eventtime,  substring(utime,1,10) eventdate, rparam alertname, rdata alertstatus,
            case when rdata = 'Normal' then 1 else 0 end normal_status,
            case when rdata = 'Low' then 1 else 0 end low_status,
            case when rdata = 'Shutdown' then 1 else 0 end shutdown_status
            from kafkaparsed a            
            where rparam in ('Battery State Change')
            {where1}        
        ) b
        on a.deveui = b.deveui
        where a.devicetype in ('Soap Dispenser', 'Paper Towel Dispenser', 'Toilet Tissue Dispenser')
        {where2}
        group by a.devicetype, a.deveui
            """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'normal_count': i[2],
                'low_count': i[3],
                'shutdown_count': i[4]
            })
    return ret


def get_device_dispense_history(io: IO, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui='{deveui}' "

    if eventdate_from:
        where += f" and  str_to_date(a.eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(a.eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""select distinct c.devicetype, a.deveui, a.eventdate, cast(coalesce(b.rollusage,'0') as DECIMAL(4,3)) product_usage,  a.dispense_count, a.recordfrom, a.recordto
            from (
                select deveui, substring(timestamp,1, 10) eventdate, count(distinct timestamp) dispense_count, min(recordnumber) recordfrom, max(recordnumber) recordto
            from kafkaparsed where rparam ='DISPENSE'
            group by deveui, substring(timestamp,1, 10)
            ) a
            left join
            (
                select substring(timestamp,1, 10) eventdate, deveui, sum(diff)/100 rollusage
            from
            (
                select deveui, gatewayid, utime, timestamp,  remaining,
                coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) last_remaining,
            case
            when
            cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED) between 0 and 15 then
            cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED)
            else 0
            end
            diff,
                cmd, param
            from kafkaparsed where
            rparam = 'Product Fuel Gauge'
            ) a
            group by substring(timestamp,1, 10), deveui
            ) b
            on a.deveui=b.deveui
            and a.eventdate = b.eventdate
            left join devicemap c
            on a.deveui = c.deveui     
            where 1=1   
            {where}
            order by a.eventdate desc
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'product_usage': i[3],
                'dispense_count': i[4],
                'recordfrom': i[5],
                'recordto': i[6]
            })
    return ret


def get_dispense_report(io: IO, devicetype: None, deveui: None, eventdate: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate:
        where += f" and a.eventdate=%(eventdate)s "

    query = f"""select distinct c.devicetype, a.deveui, a.eventdate, cast(coalesce(b.rollusage,'0') as DECIMAL(4,3)) product_usage,  a.dispense_count, a.recordfrom, a.recordto
            from (
                select deveui, substring(timestamp,1, 10) eventdate, count(distinct timestamp) dispense_count, min(recordnumber) recordfrom, max(recordnumber) recordto
            from kafkaparsed where rparam ='DISPENSE'
            group by deveui, substring(timestamp,1, 10)
            ) a
            left join
            (
                select substring(timestamp,1, 10) eventdate, deveui, sum(diff)/100 rollusage
            from
            (
                select deveui, gatewayid, utime, timestamp,  remaining,
                coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) last_remaining,
            case
            when
            cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED) between 0 and 15 then
            cast(remaining as UNSIGNED) - cast(coalesce(lead(remaining) over (partition by deveui order by recordnumber), remaining) as UNSIGNED)
            else 0
            end
            diff,
                cmd, param
            from kafkaparsed where
            rparam = 'Product Fuel Gauge'
            ) a
            group by substring(timestamp,1, 10), deveui
            ) b
            on a.deveui=b.deveui
            and a.eventdate = b.eventdate
            left join devicemap c
            on a.deveui = c.deveui     
            where 1=1   
            {where}
            order by a.eventdate desc
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate': eventdate}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventdate': i[2],
                'product_usage': i[3],
                'dispense_count': i[4],
                'recordfrom': i[5],
                'recordto': i[6]
            })
    return ret


def get_battery_fuelgauge_hist(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
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

    query = f"""select distinct b.devicetype, a.deveui, a.utime eventtime, a.eventdate, a.battery_reading 
        from 
        (
        select distinct deveui, utime, substring(utime, 1, 10) eventdate,  CONV(data, 16, 10) battery_reading
        from kafkaparsed where rparam = 'Battery Fuel Gauge'  and length(data)>1         
        {where1}             
        ) a
        left join 
        devicemap b
        on a.deveui = b.deveui
        where 1=1 
        {where2}
        order by utime 
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventtime': i[2],
                'eventdate': i[3],
                'battery_reading': i[4]
            })
    return ret




def get_fuelgauge_hist(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None, remaining: None, roll: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(eventdate, '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    if remaining:
        where += f" and remaining=%(remaining)s "

    if roll:
        where += f" and roll=%(roll)s "

    query = f"""select devicetype, deveui, eventtime, eventdate, gatewayid, recordnumber, rparam, roll, remaining
        from (
        select b.devicetype, a.deveui, utime eventtime,  substring(utime,1, 10) eventdate, gatewayid, recordnumber, 
        rparam, 
        case when b.devicetype ='Soap Dispenser' then "" else roll end roll, 
        remaining 
            from bdasdev.kafkaparsed a 
            left join bdasdev.devicemap b
            on a.deveui = b.deveui    
            where 1=1
            and rparam  in ( 'Product Fuel Gauge')    
            
        ) a
        where 1=1
        {where}
        order by eventtime   
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to, 'remaining': remaining, 'roll': roll }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventtime': i[2],
                'eventdate': i[3],
                'gatewayid': i[4],
                'recordnumber': i[5],
                'rparam': i[6],
                'roll': i[7],
                'remaining': i[8]
            })
    return ret

def get_device_fuelgauge_hist(io: IO, deveui: None, eventdate_from: None, eventdate_to: None, remaining: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "


    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    if remaining:
        where += f" and remaining=%(remaining)s "

    query = f"""select b.devicetype, a.deveui, utime eventtime,  substring(utime,1, 10) eventdate, gatewayid, recordnumber, rparam, roll, remaining 
    from kafkaparsed a 
    left join devicemap b
    on a.deveui = b.deveui    
    where 1=1
    and rparam  in ( 'Product Fuel Gauge')    
    {where} 
    order by utime  
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to, 'remaining': remaining }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventtime': i[2],
                'eventdate': i[3],
                'gatewayid': i[4],
                'recordnumber': i[5],
                'rparam': i[6],
                'roll': i[7],
                'remaining': i[8]
            })
    return ret


def get_dispenser_powerup_hist(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
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


    query = f"""select b.devicetype, a.deveui, a.utime eventtime, a.eventdate, a.eventhour
         from (
         select  deveui, utime, substring(utime, 1, 10) eventdate, substring(utime, 12, 2) eventhour
            from  kafkaparsed 
         where rparam= 'Dispenser Powerup'
         {where1}
         group by deveui, utime
        ) a
        left join devicemap  b
        on a.deveui=b.deveui
        where 1=1
        {where2}
        order by a.deveui, utime
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'eventtime': i[2],
                'eventdate': i[3],
                'eventhour': i[4]
            })
    return ret

def get_dispenser_coverevent_lag(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where = ""
    if deveui:
        where += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where += f" and b.devicetype=%(devicetype)s "

    if eventdate_from:
        where += f" and  str_to_date(substring(eventtime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(eventtime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""select b.devicetype, a.deveui, recordnumber, eventtime, rparam eventtype, rdata, lead_rdata, second_diff
                from
                (
                    select distinct deveui,  recordnumber, timestamp eventtime, rparam, rdata,
                    lead(rdata,1) over(partition by deveui order by recordnumber desc) lead_rdata,
                    lead(timestamp,1) over(partition by deveui order by recordnumber desc) lead_timestamp,
                    TIMESTAMPDIFF(SECOND,  cast(lead(timestamp,1) over(partition by deveui order by recordnumber desc) as datetime), 
                    cast(timestamp as datetime)) second_diff
                    from kafkaparsed where rparam='Cover Event' 
                ) a
                left join 
                devicemap b
                on a.deveui=b.deveui
                where rdata='Cover Close' and lead_rdata='Cover Open'
                {where}
                order by a.deveui, eventtime
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'recordnumber': i[2],
                'eventtime': i[3],
                'eventtype': i[4],
                'rdata': i[5],
                'lead_rdata': i[6],
                'second_diff': i[7]
            })
    return ret

def get_dispenser_coverevent_agg(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    where2 = ""
    if deveui:
        where2 += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where2 += f" and a.devicetype=%(devicetype)s "

    if eventdate_from:
        where1 += f" and  str_to_date(substring(eventtime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where1 += f" and  str_to_date(substring(eventtime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""
        select devicetype, deveui, 
        count(distinct  case when recordnumber>0 then recordnumber end) count,
        cast(avg(second_diff) as unsigned) avg_second_diff
        from
        (        
            select a.devicetype, a.deveui, coalesce(b.recordnumber,-1) recordnumber, coalesce(b.eventtime,'') eventtime,
            coalesce(b.rparam, 'Cover Event') rapram, coalesce(rdata, 'Cover Open') rdata, 
            coalesce(lead_rdata, 'Cover Close') lead_rdata, coalesce(b.second_diff, 0) second_diff
            from (
            select deveui, devicetype from devicemap
            where manufacturer='GeorgiaPacific'
            ) a
            left join 
            (
            select deveui, recordnumber, eventtime, rparam, rdata, lead_rdata, second_diff
            from
            (
                select distinct deveui,  recordnumber, timestamp eventtime, rparam, rdata,
                    lead(rdata,1) over(partition by deveui order by recordnumber desc) lead_rdata,
                    lead(timestamp,1) over(partition by deveui order by recordnumber desc) lead_timestamp,
                    TIMESTAMPDIFF(SECOND,  cast(lead(timestamp,1) over(partition by deveui order by recordnumber desc) as datetime), 
                    cast(timestamp as datetime)) second_diff
                    from kafkaparsed where rparam='Cover Event' 
            ) x
            where rdata='Cover Close' and lead_rdata='Cover Open'	
            {where1}
            ) b
            on a.deveui = b.deveui
        ) a
        where 1=1
        {where2}
        group by devicetype, deveui
        """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to }).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'count': i[2],
                'avg_second_diff': i[3]
            })
    return ret