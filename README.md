# Connected Restroom API

## Swagger Documentation.
Here is swagger link. https://sirendev.westus.cloudapp.azure.com/api/v1/docs

## Master Data API Endpoint.
      /common/devicetype            // list device type. 
      /common/devices               // list devices with device type.
      /common/eventtype             // list event type.  
      /common/deviceeventtype       // event type reported in given date range for a given device. 
      

## Dispensers
      /gp/aggregate/productcard                   // aggregate product metrics per device.
      /gp/aggregate/batterycard                   // aggregate battery metrics per device.
      /gp/aggregate/daily/productalert            // device product alert dailly aggregate.   
      /gp/aggregate/daily/productusage            // device prouduct usage/consumption daily aggregate.
      /gp/aggregate/daily/dispense                // device dispense daily aggregate.
      /gp/aggregate/daily/batteryalert            // device battery alert dailly aggregate.   

Event detail are captured in below endpoint.

      /gp/productalert                            //  device product alerts in timeseries.
      /gp/batteryalert                            //  deviice battery alerts in timeseries.
      /gp/productfuelgaugehistory                 //  device products remaining (%) in timeseries.
      /gp/batteryfuelgaugehistory                 //  devicie battery reamining (%) in timeseries.

## Valves 
    /zurn/alertstatus                             //  flush/faucet valve alerts in timeseries.
    /zurn/aggregate/daily/activation              //  flush/faucet valve activation daily aggregate.
    /zurn/aggregate/activation                    //  flush/faucet valve activation aggregate per device.
      
