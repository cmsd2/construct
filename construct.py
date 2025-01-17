from __future__ import print_function

import json
import os
import pprint
import random
import requests
import sh
import time
from threading import Thread
from async_framework import FrameworkMessage

# See KillTaskMessage in include/mesos/v1/scheduler/scheduler.proto
SUBSCRIBE_BODY = {
    "type": "SUBSCRIBE",
    "subscribe": {
        "framework_info": {
            "user" :  "vagrant",
            "name" :  "Example HTTP Framework"
        },
        "force" : True
    }
}

#### **NOTE**
#
# Even though framework_id is defined as "optional" in scheduler.proto, it MUST
# always be present:
#      optional FrameworkID framework_id = 1;
#
# in all Call messages, apart from the SUBSCRIBE - because we don't have an ID
# before subscribing (which is why it's defined as `optional`).


# See KillTaskMessage in include/mesos/v1/scheduler/scheduler.proto
TEARDOWN_BODY = {
    "type": "TEARDOWN",
    "framework_id": {
        "value" : None
    }
}

# See KillTaskMessage in include/mesos/v1/scheduler/scheduler.proto
KILLTASK_BODY = {
    "type": "KILL",
    "framework_id": {
        "value" : None
    },
    "kill": {
        "agent_id": {"value": None},
        "task_id": {"value": None}
    }
}


DOCKER_JSON = "../resources/container.json"
LAUNCH_JSON = "../resources/launch.json"
TASK_RESOURCES_JSON = "../resources/task_resources.json"


# Adjust the ports according to how you launched Mesos:
# see --port in the commands in "Prerequisites"
MASTER_URL = 'http://192.168.33.10:5050'
SLAVE_URL = 'http://192.168.33.11:5051'
API_V1 = '/api/v1/scheduler'
API_URL = '{}/{}'.format(MASTER_URL, API_V1)
CONTENT = 'application/json'

headers = {
    "Content-Type": CONTENT, 
    "Accept": CONTENT, 
    "Connection": "close"
}

pretty = pprint.PrettyPrinter(indent=2)

def get_json(filename):
    """ Loads the JSON from the given filename."""
    with open(filename) as jsonfile:
        lines = jsonfile.readlines()

    return json.loads("".join(lines))

class ApiConnector:
    def __init__(self):
        # TODO: THIS IS THREAD-UNSAFE
        self.terminate = False
        self.offers = []
        self.framework_id = None
        self.last_heartbeat = None
        self.tasks = {}

    def get_offers(self):
        return self.offers

    def handle_heartbeat(self, body, queue):
        print("[HEARTBEAT] {}".format(body))
        self.last_heartbeat = time.ctime()
        queue.put(FrameworkMessage("heartbeat", body))

    def handle_error(self, body, queue):
        print("[ERROR] {}".format(body))
        queue.put(FrameworkMessage("error", body))

    def handle_offers(self, body, queue):
        print("[OFFERS] {}".format(body))
        self.offers = body.get("offers")
        queue.put(FrameworkMessage("offers", body))

    def handle_update(self, body, queue):
        print("[UPDATE] {}".format(body))
        task_id = body["update"]["status"]["task_id"]["value"]
        self.tasks[task_id] = body
        queue.put(FrameworkMessage("update", body))
        
    def handle_subscribed(self, url, body, queue):
        framework_id = body.get("subscribed").get("framework_id").get("value")
        self.framework_id = framework_id
        if framework_id:
            print("Framework {} registered with Master at ({})".format(framework_id, url))
        queue.put(FrameworkMessage("subscribed", body))

    def post(self, url, body, queue, **kwargs):
        """ POST `body` to the given `url`.
        
        @return: the Response from the server.
        @rtype: requests.Response
        """
        import time
        print('Connecting to Master: ' + url)
        r = requests.post(url, headers=headers, data=json.dumps(body), **kwargs)
    
        if r.status_code not in [200, 202]:
            raise ValueError("Error sending request: {} - {}".format(r.status_code, r.text))
        if 'stream' in kwargs:
            # The streaming format needs some munging:
            first_line = True
            for line in r.iter_lines():
                if first_line:
                    count_bytes = int(line)
                    first_line = False
                    continue
                body = json.loads(line[:count_bytes])
                count_bytes = int(line[count_bytes:])
                body_type = body.get("type")
                if body_type == "HEARTBEAT":
                    self.handle_heartbeat(body, queue)
                elif body_type == "ERROR":
                    self.handle_error(body, queue)
                # When we get OFFERS we want to see them (and eventually, use them)
                elif body_type == "OFFERS":
                    self.handle_offers(body, queue)
                # We need to capture the framework_id to use in subsequent requests.
                elif body_type == "SUBSCRIBED":
                    self.handle_subscribed(url, body, queue)
                elif body_type == "UPDATE":
                    self.handle_update(body, queue)
                elif self.terminate:
                    return
                else:
                    print("unrecognised message: {}".format(body))
                    queue.put(FrameworkMessage("unknown", body))
        return r

    def get_framework(self, index=None, id=None):
        """Gets information about the given Framework.
        
        From the `/state.json` endpoint (soon to be deprecated, in favor of `/state`)
        we retrieve the Framework information.
        
        Can only specify one of either `index` or `id`.
        
        @param index: the index in the array of active frameworks
        @param id: the framework ID
        @return: the full `FrameworkInfo` structure
        @rtype: dict
        """
        if index and id:
            raise ValueError("Cannot specify both ID and Index")
        r = requests.get("{}/state.json".format(MASTER_URL))
        master_state = r.json()
        frameworks = master_state.get("frameworks")
        if frameworks and isinstance(frameworks, list):
            if index is not None and len(frameworks) > index:
                return frameworks[index]
            elif id:
                for framework in frameworks:
                    if framework.get("id") == id:
                        return framework


    def register_framework(self, queue):
        channel = None
        try:
            channel = ApiConnectorThread(self, queue)
            channel.start()
            print("The background channel was started to {}".format(API_URL))
        except Exception, ex:
            print("An error occurred: {}".format(ex))
        return channel

    
    def terminate_framework(self, fid=None):
        if not fid:
            framework = self.get_framework(0)
            if framework:
                fid = framework['id']
            else:
                print("No frameworks to terminate")
        body = TEARDOWN_BODY
        body['framework_id']['value'] = fid
        self.post(API_URL, body, None)

    def close_channel(self):
        print("Stopping connector thread")
        self.terminate = True
        self.framework_id = None
        self.offers = None

        
class ApiConnectorThread(Thread):
    def __init__(self, connector, queue):
        super(ApiConnectorThread, self).__init__()
        self.connector = connector
        self.daemon = True
        self.timeout = 30
        self.queue = queue

        
    def run(self):
        """Subscribe to mesos events and handle offers"""
        kwargs = {'stream':True, 'timeout':self.timeout}
        ret = self.connector.post(API_URL, SUBSCRIBE_BODY, self.queue, **kwargs)
        print("Subscribe post request returned: {}".format(ret))

        
    def close_channel(self):
        print("Stopping connector thread")
        self.connector.close_channel()

        time.sleep(5)
        print("Channel was closed: {}".format(self.is_alive()))


    
def main():
    r = requests.get("{}/state.json".format(MASTER_URL))
    master_state = r.json()

    r = requests.get("{}/state.json".format(SLAVE_URL))
    slave_state = r.json()

    # If this is not true, you're in for a world of hurt:
    assert master_state["version"] == slave_state["version"]
    print("Mesos version running at {}".format(master_state["version"]))

    conn = ApiConnector()
    
    # And right now there ought to be no frameworks:
    assert conn.get_framework(index=0) is None

    background_thread = conn.register_framework()

    background_thread.join()


    
if __name__ == '__main__':
    main()

    
