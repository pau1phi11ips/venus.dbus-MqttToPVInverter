#!/usr/bin/env python

"""
Changed a lot of a Script originally created by Ralf Zimmermann (mail@ralfzimmermann.de) in 2020.
The orginal code and its documentation can be found on: https://github.com/RalfZim/venus.dbus-fronius-smartmeter
Used https://github.com/victronenergy/velib_python/blob/master/dbusdummyservice.py as basis for this service.
"""

"""
/data/Pathtothisscript/vedbus.py
/data/Pathtothisscript/ve_utils.py
python -m ensurepip --upgrade
pip install paho-mqtt
"""
try:
  import gobject  # Python 2.x
except:
  from gi.repository import GLib as gobject # Python 3.x
from asyncio.windows_events import NULL
import platform
import logging
import time
import sys
import json
import os
import paho.mqtt.client as mqtt
try:
  import thread   # for daemon = True  / Python 2.x
except:
  import _thread as thread   # for daemon = True  / Python 3.x

# our own packages
sys.path.insert(1, os.path.join(os.path.dirname(__file__), '../ext/velib_python'))
from vedbus import VeDbusService

path_UpdateIndex = '/UpdateIndex'

# MQTT Setup
broker_address = "192.168.1.134"
MQTTNAME = "MQTTtoMeter"
sensor_path = "N/b827eb278b4b/pvinverter"

# set variables
connected = 0
passthru = 0
maxcellvoltage = 3.0
powercurr = 0
totalin = 0
totalout = 0


# MQTT Abfragen:

def on_disconnect(client, userdata, rc):
    global connected
    print("Client Got Disconnected")
    if rc != 0:
        print('Unexpected MQTT disconnection. Will auto-reconnect')

    else:
        print('rc value:' + str(rc))

    try:
        print("Trying to Reconnect")
        client.connect(broker_address)
        connected = 1
    except Exception as e:
        logging.exception("Fehler beim reconnecten mit Broker")
        print("Error in Retrying to Connect with Broker")
        connected = 0
        print(e)

def on_connect(client, userdata, flags, rc):
        global connected
        if rc == 0:
            print("Connected to MQTT Broker!")
            connected = 1
            client.subscribe(sensor_path)
        else:
            print("Failed to connect, return code %d\n", rc)


def on_message(client, userdata, msg):
    try:

        global powercurr, totalin, totalout
        if msg.topic == sensor_path:   # JSON String from MQTT
            if msg.payload != '{"value": null}' and msg.payload != b'{"value": null}':
                jsonpayload = json.loads(msg.payload)
                powercurr = float(jsonpayload["Zaehler"]["Power_curr"])
                totalin = float(jsonpayload["Zaehler"]["Total_in"])
                totalout = float(jsonpayload["Zaehler"]["Total_out"])
            else:
                print("Response from MQTT was null and ignored")

    except Exception as e:
        logging.exception("Program MQTTtoMeter crashed. (on message function)")
        print(e)
        print("Something went wrong in the MQTTtoMeter program when reading the messages")




class DbusDummyService:
  def __init__(self, servicename, deviceinstance, paths, productname='ET112', connection='MQTT'):
    self._dbusservice = VeDbusService(servicename)
    self._paths = paths

    logging.debug("%s /DeviceInstance = %d" % (servicename, deviceinstance))

    # Create the management objects, as specified in the ccgx dbus-api document
    self._dbusservice.add_path('/Mgmt/ProcessName', __file__)
    self._dbusservice.add_path('/Mgmt/ProcessVersion', 'Unkown version, and running on Python ' + platform.python_version())
    self._dbusservice.add_path('/Mgmt/Connection', connection)

    # Create the mandatory objects
    self._dbusservice.add_path('/DeviceInstance', deviceinstance)
    self._dbusservice.add_path('/ProductId', 45069) # value used in ac_sensor_bridge.cpp of dbus-cgwacs
    self._dbusservice.add_path('/ProductName', productname)
    self._dbusservice.add_path('/FirmwareVersion', 0.1)
    self._dbusservice.add_path('/HardwareVersion', 0)
    self._dbusservice.add_path('/Connected', 1)

    for path, settings in self._paths.items():
      self._dbusservice.add_path(
        path, settings['initial'], writeable=True, onchangecallback=self._handlechangedvalue)

    gobject.timeout_add(1000, self._update) # pause 1000ms before the next request

  
  
  
  def _update(self):
    self._dbusservice['/Ac/Power'] =  powercurr # positive: consumption, negative: feed into grid
    self._dbusservice['/Ac/Current'] = round(powercurr / 230 ,2)
    self._dbusservice['/Ac/Voltage'] = 230
    self._dbusservice['/Ac/L1/Voltage'] = 230
    self._dbusservice['/Ac/L1/Current'] = round(powercurr / 230 ,2)
    self._dbusservice['/Ac/L1/Power'] = round(powercurr, 2)
    self._dbusservice['/Ac/L2/Voltage'] = NULL
    self._dbusservice['/Ac/L3/Voltage'] = NULL
    self._dbusservice['/Ac/L2/Power'] = NULL
    self._dbusservice['/Ac/L3/Power'] = NULL
    self._dbusservice['/Ac/L2/Current'] = NULL
    self._dbusservice['/Ac/L3/Current'] = NULL
    self._dbusservice['/Ac/Energy/Forward'] = totalin

    logging.info("PV Output Consumption: {:.0f}".format(powercurr))
    # increment UpdateIndex - to show that new data is available
    index = self._dbusservice[path_UpdateIndex] + 1  # increment index
    if index > 255:   # maximum value of the index
      index = 0       # overflow from 255 to 0
    self._dbusservice[path_UpdateIndex] = index
    return True

  def _handlechangedvalue(self, path, value):
    logging.debug("someone else updated %s to %s" % (path, value))
    return True # accept the change

def main():
  logging.basicConfig(level=logging.DEBUG) # use .INFO for less logging
  thread.daemon = True # allow the program to quit

  from dbus.mainloop.glib import DBusGMainLoop
  # Have a mainloop, so we can send/receive asynchronous calls to and from dbus
  DBusGMainLoop(set_as_default=True)
  
  pvac_output = DbusDummyService(
    servicename='com.victronenergy.pvinverter.cgwacs_ttyUSB0_mb1',
    deviceinstance=0,
    paths={
      '/Ac/Current': {'initial': 0},        
      '/Ac/Energy/Forward': {'initial': 0},
      '/Ac/L1/Voltage': {'initial': 0},
      '/Ac/L2/Voltage': {'initial': 0},
      '/Ac/L3/Voltage': {'initial': 0},
      '/Ac/L1/Current': {'initial': 0},
      '/Ac/L2/Current': {'initial': 0},
      '/Ac/L3/Current': {'initial': 0},
      '/Ac/L1/Power': {'initial': 0},
      '/Ac/L2/Power': {'initial': 0},
      '/Ac/L3/Power': {'initial': 0},
      '/Ac/Power': {'initial': 0},
      '/Ac/Voltage': {'initial': 0},
      path_UpdateIndex: {'initial': 0},
    })

  logging.info('Connected to dbus, and switching over to gobject.MainLoop() (= event based)')
  mainloop = gobject.MainLoop()
  mainloop.run()

# Configuration MQTT
client = mqtt.Client(MQTTNAME) # create new instance
client.on_disconnect = on_disconnect
client.on_connect = on_connect
client.on_message = on_message
client.connect(broker_address)  # connect to broker

client.loop_start()

if __name__ == "__main__":
  main()