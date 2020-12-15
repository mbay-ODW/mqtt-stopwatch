import paho.mqtt.client as mqtt
import sys
import logging
import time
import datetime


logger = logging.getLogger('Operation Listener')
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger.info('Logger on MQTT for DeviceControl was initialised')

broker = "10.0.1.8"
topic = "#"
avg = []
counter = 0

timeCurrentEvent = datetime.datetime.now()
timePreviousEvent = datetime.datetime.now()


def Average(lst):
    return round(sum(lst) / len(lst),2)

def on_message_msgs(mosq, obj, msg):
    global avg
    global counter
    global timeCurrentEvent
    global timePreviousEvent
    timeCurrentEvent = datetime.datetime.now()
    counter = counter + 1
    # This callback will only be called for messages with topics that matchs the assigned topics
    #logger.debug('Callback function was initiated')
    #logger.info('The following payload arrived: %s', msg.payload)
    #logger.debug('Object with Event-Class will be created')
    timeDelta = timeCurrentEvent - timePreviousEvent
    timePreviousEvent = timeCurrentEvent
    avg.append(timeDelta.total_seconds()*1000)
    if counter == 50:
        counter = 0
        print("Average time between messages in ms: %s"%str(Average(avg)))
        avg = []
    

def main():
    try:
        logger.debug('Setting prefix within MQTT broker for machine from config file')
        logger.debug('Initialising MQTT client with loaded credentials for listener')
        client = mqtt.Client()
        logger.info('MQTT client with loaded credentials was initialised')
        client.message_callback_add(topic, on_message_msgs)
        logger.info('Connecting to MQTT Broker')
        client.connect(broker, 1883, 60)
        client.subscribe("#")
        logger.info('Start Loop forever and listening')
        client.loop_forever()
    except Exception as e:
        logger.error('The following error occured: ' + str(e))
        client.stop_loop()
        logger.warning('Loop forever stopped, disconnecting')
        client.disconnect()
        logger.debug('disconnected')

def start():
    try:
        logger.info('Initially starting triggering of getting all pending operations')
        while True:
            logger.debug(
                'Starting main loop')
            main()
            logger.error('Main loop left')
            time.sleep(10)
        logger.error('Main loop left')
    except KeyboardInterrupt:
        sys.exit(1)
    except Exception as e:
        logger.error('The following error occured: ' + str(e))
        pass

start()
