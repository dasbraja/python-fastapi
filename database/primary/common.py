from config import IO
from logger import *


def get_device_type(io: IO):

    query = f"""select distinct devicetype from devicemap
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query).fetchall():
            ret.append({
                'devicetype': i[0]
            })
    return ret

def get_devices(io: IO, devicetype: None, deveui: None, location: None, manufacturer: None, store: None):

    where = ""
    if devicetype:
        where += f" and devicetype=%(devicetype)s "
    if deveui:
        where += f" and deveui=%(deveui)s "

    if location:
        where += f" and restroom_location=%(location)s "

    if manufacturer:
        where += f" and manufacturer=%(manufacturer)s "

    if store:
        where += f" and store=%(store)s "

    query = f"""select distinct devicetype, deveui, restroom_location location, manufacturer, store from devicemap where 1=1 {where}
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query,{'devicetype': devicetype, 'deveui': deveui,'location': location, 'manufacturer': manufacturer,  'store': store}).fetchall():
            ret.append({
                'devicetype': i[0],
                'deveui': i[1],
                'location': i[2],
                'manufacturer': i[3],
                'store': i[4]
            })
    return ret

def get_device_event(io: IO, devicetype: None, deveui: None, eventdate_from: None, eventdate_to: None):

    where1 = ""
    where2 = ""
    if deveui:
        where1 += f" and a.deveui=%(deveui)s "
    elif devicetype:
        where1 += f" and a.devicetype=%(devicetype)s "

    if eventdate_from:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""    select dense_rank() over(order by deveui, b.rparam) recordid,
                a.devicetype, a.deveui, b.rparam    
                from  devicemap a
                 left join (
                    select rparam, deveui
                    from kafkaparsed 
                    where 1=1
                    {where2}    
                    group by rparam, deveui 
                    union 
                    select message_type rparam, deveui
                    from zurnflush
                    where 1=1
                    {where2} 
                    group by message_type, deveui
                    union 
                    select message_type rparam, deveui
                    from zurnfaucet
                    where 1=1
                    {where2}
                    group by message_type, deveui
                ) b
                on a.deveui = b.deveui
                where 1=1
                {where1}

                """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query,{'devicetype': devicetype, 'deveui': deveui, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'recordid': i[0],
                'devicetype': i[1],
                'deveui': i[2],
                'eventtype': i[3]
            })
    return ret

def get_eventtype(io: IO, devicetype: None, eventtype: None):

    where = ""
    if devicetype:
        where += f" and devicetype=%(devicetype)s "
    if eventtype:
        where += f" and eventtype=%(eventtype)s "

    query = f"""select distinct eventtype, devicetype
        from (
        select distinct rparam eventtype, deveui
        from kafkaparsed
        union all
        select distinct message_type eventtype,  deveui 
        from zurnflush 
        union all
        select distinct message_type eventtype,  deveui 
        from zurnfaucet 
        ) a
        left join 
        (select distinct deveui, devicetype from devicemap) b
        on a.deveui = b.deveui
        where 1=1
        {where}
        order by eventtype 
            """
    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query,{'devicetype': devicetype, 'eventtype': eventtype}).fetchall():
            ret.append({
                'eventtype': i[0],
                'devicetype': i[1]
            })
    return ret


def get_aggregate_devicetype(io: IO, devicetype: None, eventdate_from: None, eventdate_to: None):
    where1 = ""
    where2 = ""
    if devicetype:
        where1 += f" and a.devicetype=%(devicetype)s "

    if eventdate_from:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where2 += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"


    query = f"""
    select a.devicetype, 
        coalesce(b.reported_device_count,0) reported_device_count,
        coalesce(c.active_device_count,0) active_device_count,
        coalesce(d.active_alert_device_count , 0) active_alert_device_count
        from 
        (
            select distinct devicetype from devicemap
        ) a
        left join 
        (
            select  coalesce(devicetype, 'All') devicetype, count(*)  reported_device_count
            from
            (
                select distinct b.devicetype, a.deveui from 
                (
                    select distinct deveui
                    from zurnflush
                    where 1=1 
                    {where2} 
                    union
                    select distinct deveui
                    from zurnfaucet
                    where 1=1 
                    {where2}                      
                    union
                    select distinct deveui
                    from kafkaparsed
                    where  rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
                    {where2}
                ) a
                left join devicemap b
                on a.deveui = b.deveui
                ) x
            group by devicetype
        ) b
        on a.devicetype = b.devicetype
        left join 
        (
        select  coalesce(devicetype, 'All') devicetype, count(*)  active_device_count
        from
        (
            select distinct b.devicetype, a.deveui from 
            (
                select distinct deveui
                from zurnflush
                where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                                  
                union
                select distinct deveui
                from zurnfaucet
                where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                 
                union
                select distinct deveui
                from kafkaparsed
                where rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
                and cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)
            ) a
        left join devicemap b
        on a.deveui = b.deveui
        ) x
        group by devicetype
        ) c
        on a.devicetype = c.devicetype
        left join (
            select b.devicetype, count(distinct a.deveui) active_alert_device_count 
            from kafkaparsed a 
            left join devicemap b
            on a.deveui = b.deveui
            where rparam in ('Product Low Alert')
            and alertstatus ='sent'
            {where2}              
            group by  b.devicetype                 
        ) d
        on a.devicetype = d.devicetype  
        where 1=1
        {where1}
            """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'devicetype': devicetype, 'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'devicetype': i[0],
                'reported_device_count': i[1],
                'active_device_count': i[2],
                'active_alert_device_count': i[3]

            })
    return ret




def get_aggregate_all(io: IO,  eventdate_from: None, eventdate_to: None):

    where =""
    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""
        
select sum(reported_device_count) reported_device_count, sum(active_device_count) active_device_count, 
sum(active_alert_device_count) active_alert_device_count
        from 
        (
            select a.devicetype, 
            coalesce(b.reported_device_count,0) reported_device_count,
            coalesce(c.active_device_count,0) active_device_count,
            coalesce(d.active_alert_device_count , 0) active_alert_device_count 
            from 
            (
                select distinct devicetype from devicemap
            ) a
            left join 
            (
                select  coalesce(devicetype, 'All') devicetype, count(*)  reported_device_count
                from
                (
                    select distinct b.devicetype, a.deveui from 
                    (
                        select distinct deveui
                        from zurnflush
                        where  1 =1 
                        {where} 
                        union
                        select distinct deveui
                        from zurnfaucet
                        where  1 =1 
                        {where}                         
                        union
                        select distinct deveui
                        from kafkaparsed
                        where  rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
                        {where}
                    ) a
                    left join devicemap b
                    on a.deveui = b.deveui
                    ) x
                group by devicetype
            ) b
            on a.devicetype = b.devicetype
            left join 
            (
            select  coalesce(devicetype, 'All') devicetype, count(*)  active_device_count
            from
            (
                select distinct b.devicetype, a.deveui from 
                (
                    select distinct deveui
                    from zurnflush
                    where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                      
                    union
                    select distinct deveui
                    from zurnfaucet
                    where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                      
                    union
                    select distinct deveui
                    from kafkaparsed
                    where rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
                    and cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)
                ) a
            left join devicemap b
            on a.deveui = b.deveui            
            ) x            
            group by devicetype
            ) c
            on a.devicetype = c.devicetype
            
            left join (
				select b.devicetype, count(distinct a.deveui) active_alert_device_count 
                from kafkaparsed a 
                left join devicemap b
                on a.deveui = b.deveui
				where rparam in ('Product Low Alert')
				 and alertstatus ='sent'
				 {where}  				  
                group by  b.devicetype                 
            ) d
            on a.devicetype = d.devicetype            
        ) z
            """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'reported_device_count': i[0],
                'active_device_count': i[1],
                'active_alert_device_count': i[2]
            })
    return ret


def get_aggregate_messagetype(io: IO, eventdate_from: None, eventdate_to: None):
    where = ""
    if eventdate_from:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') >= str_to_date('{eventdate_from}', '%%Y-%%m-%%d')"

    if eventdate_to:
        where += f" and  str_to_date(substring(utime,1,10), '%%Y-%%m-%%d') <= str_to_date('{eventdate_to}', '%%Y-%%m-%%d')"

    query = f"""
            WITH
cte as (
	select a.devicetype, 
			coalesce(b.reported_device_count,0) reported_device_count,
			coalesce(c.active_device_count,0) active_device_count,
			coalesce(d.active_alert_device_count , 0) active_alert_device_count
			from 
			(
				select distinct devicetype from bdasdev.devicemap
			) a
			left join 
			(
				select  coalesce(devicetype, 'All') devicetype, count(*)  reported_device_count
				from
				(
					select distinct b.devicetype, a.deveui from 
					(
						select distinct deveui
						from zurnflush
						where 1=1
						{where}  
						union
						select distinct deveui
						from zurnfaucet
						where 1=1 
						{where} 
						union
						select distinct deveui
						from kafkaparsed
						where  rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
						{where} 
					) a
					left join devicemap b
					on a.deveui = b.deveui
					) x
				group by devicetype
			) b
			on a.devicetype = b.devicetype
			left join 
			(
			select  coalesce(devicetype, 'All') devicetype, count(*)  active_device_count
			from
			(
				select distinct b.devicetype, a.deveui from 
				(
					select distinct deveui
					from zurnflush
					where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                                  
					union
					select distinct deveui
					from zurnfaucet
					where cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)                 
					union
					select distinct deveui
					from kafkaparsed
					where rparam in ('Dispense', 'Battery Fuel Gauge', 'Product Fuel Gauge', 'Battery State Change', 'Dispenser Powerup', 'Product Low Alert')
					and cast(substring(utime,1, 10) as date) >= date_add(curdate(), INTERVAL  -3 DAY)
				) a
			left join devicemap b
			on a.deveui = b.deveui
			) x
			group by devicetype
			) c
			on a.devicetype = c.devicetype
			left join (
				select b.devicetype, count(distinct a.deveui) active_alert_device_count 
				from kafkaparsed a 
				left join devicemap b
				on a.deveui = b.deveui
				where rparam in ('Product Low Alert')
				and alertstatus ='sent'
				{where} 
		
				group by  b.devicetype                 
			) d
			on a.devicetype = d.devicetype  
			where 1=1
)
select 
	message_type, sum(soap_dispenser_count) soap_dispenser_count, sum(papertowel_dispenser_count) papertowel_dispenser_count, 
    sum(toiletpaper_dispenser_count) toiletpaper_dispenser_count,
    sum(flushvalve_count) flushvalve_count, sum(faucet_count) faucet_count
    from (
		select 'Reported Devices' message_type,
		case when devicetype='Soap Dispenser' then reported_device_count else 0 end soap_dispenser_count,
		case when devicetype='Paper Towel Dispenser' then reported_device_count else 0 end papertowel_dispenser_count,
		case when devicetype='Toilet Tissue Dispenser' then reported_device_count else 0 end toiletpaper_dispenser_count,
		case when devicetype='Flush Valve' then reported_device_count else 0 end flushvalve_count,
		case when devicetype='Faucet' then reported_device_count else 0 end faucet_count
		from cte
	) a
    group by message_type
union 
select 
	message_type, sum(soap_dispenser_count) soap_dispenser_count, sum(papertowel_dispenser_count) papertowel_dispenser_count, 
    sum(toiletpaper_dispenser_count) toiletpaper_dispenser_count,
    sum(flushvalve_count) flushvalve_count, sum(faucet_count) faucet_count
    from (
		select 'Active Devices' message_type,
		case when devicetype='Soap Dispenser' then active_device_count else 0 end soap_dispenser_count,
		case when devicetype='Paper Towel Dispenser' then active_device_count else 0 end papertowel_dispenser_count,
		case when devicetype='Toilet Tissue Dispenser' then active_device_count else 0 end toiletpaper_dispenser_count,
		case when devicetype='Flush Valve' then active_device_count else 0 end flushvalve_count,
		case when devicetype='Faucet' then active_device_count else 0 end faucet_count
		from cte
	) a
    group by message_type
union 
select 
	message_type, sum(soap_dispenser_count) soap_dispenser_count, sum(papertowel_dispenser_count) papertowel_dispenser_count, 
    sum(toiletpaper_dispenser_count) toiletpaper_dispenser_count,
    sum(flushvalve_count) flushvalve_count, sum(faucet_count) faucet_count
    from (
		select 'Active Alert Devices' message_type,
		case when devicetype='Soap Dispenser' then active_alert_device_count else 0 end soap_dispenser_count,
		case when devicetype='Paper Towel Dispenser' then active_alert_device_count else 0 end papertowel_dispenser_count,
		case when devicetype='Toilet Tissue Dispenser' then active_alert_device_count else 0 end toiletpaper_dispenser_count,
		case when devicetype='Flush Valve' then active_alert_device_count else 0 end flushvalve_count,
		case when devicetype='Faucet' then active_alert_device_count else 0 end faucet_count
		from cte
	) a
    group by message_type

            """

    ret = []
    log.info(query)
    with io.primary_engine.connect() as conn:
        for i in conn.execute(query, {'eventdate_from': eventdate_from, 'eventdate_to': eventdate_to}).fetchall():
            ret.append({
                'message_type': i[0],
                'soap_dispenser_count': i[1],
                'papertowel_dispenser_count': i[2],
                'toiletpaper_dispenser_count': i[3],
                'flushvalve_count': i[4],
                'faucet_count': i[5]
            })
    return ret
